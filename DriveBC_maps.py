"""
Create maps from DriveBC data

Description:
0. Set working directory
1. Query data from DriveBC, write to CSV
    - 'records' parameter can be used to define query limit (default is 500)
2. Create a geodatabase from .CSV of DriveBC data
3. Import data to pre-built ArcGIS Pro project, export as .pdf maps

Inputs and environment:
Working directory - select a folder to store output and intermediate data
ArcGIS Project - extract 'DriveBC_prj.zip' to the working directory. 
The 'DriveBC_prj' folder contains the project file with the needed map and layout.
Pandas library - required

Russell Vinegar
2021-12-11
Python version 3.7.10
"""
def getdata(records = "500"):

    csv_path = 'jsonoutput.csv'

    # function to download data to csv
    def dataquery():
        import requests
        import pandas as pd

        # query data
        print("Querying DriveBC API...", end='')
        params = {
        "limit": records,
        }
        url = 'https://api.open511.gov.bc.ca/events'

        data_dl = requests.get(url, params)
        jsondata = data_dl.json() # convert to JSON object

        # flatten the nested dictionary 
        df = pd.json_normalize(jsondata, record_path =['events'])
        #print(df)

        # remove unnecessary columns
        del df['jurisdiction_url'], df['+ivr_message'], df['+linear_reference_km']
        try:
            del df['schedule.recurring_schedules']
        except:
            a = 1

        # convert event_subtype values into a string
        df['event_subtypes'] = df['event_subtypes'].apply(lambda x: str(x)) # apply method wants to use a function, lambda defines it in place
        df['event_subtypes'] = df['event_subtypes'].apply(lambda x: x[2:-2])
        
        # rename columns with dots in them
        df = df.rename({'schedule.intervals': 'schedule_intervals', 'geography.type': 'geography_type', 'geography.coordinates': 'geography_coordinates'}, axis=1)

        # export to CSV
        df.to_csv (csv_path)

        print(df.shape[0], "records retrieved")

    # set working directory
    import os
    print("Current working directory:", os.getcwd())
    change = input("Change working directory? (Y/N)")
    while change.lower() == 'y':
        newdir = input("Provide new directory path:")
        if os.path.isdir(newdir):
            os.chdir(newdir)
            print("Working directory set to", newdir)
            change = 'n'
        else:
            print("Invalid path")

    ### DOWNLOAD DRIVEBC DATA ###
    print('QUERY DATA')
    import time
    if os.path.isfile(csv_path):
        # Get file's Last modification time stamp only in terms of seconds since epoch 
        modTimesinceEpoc = os.path.getmtime(csv_path)
        # Convert seconds since epoch to readable timestamp
        modificationTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modTimesinceEpoc))
        print("Last Modified Time : ", modificationTime )

        update = input('DriveBC data already exists. Update now? (Y/N)')
        if update.lower() == 'y':
            dataquery()
            print("Data updated at", time.strftime("%H:%M:%S", time.localtime()))
    else:
        dataquery()
        print("Data updated at", time.strftime("%H:%M:%S", time.localtime()))
# set query limit if desired:
getdata(records="500")

def csv2gdb():
    import os
    print('CONVERT DATA TO GEODATABASE')
    ### CREATE GEODATABASE ###
    print('Creating geodatabase...', end='')
    import arcpy
    out_folder_path = os.getcwd()
    gdb_path = "driveBC.gdb"

    # create file geodatabase if not already there
    if os.path.isdir(gdb_path) == False:
        arcpy.CreateFileGDB_management(out_folder_path, gdb_path)
        print('gdb created')
    else:
        print('gdb already exists')

    ### CREATE FEATURE CLASSES ###
    # set environments
    import arcpy
    working_path = os.getcwd()+r"\driveBC.gdb"
    arcpy.env.workspace = working_path 
    arcpy.env.overwriteOutput = True

    # delete existing feature classes
    print('Deleting existing feature classes...', end='')
    try:
        arcpy.Delete_management('pts')
        arcpy.Delete_management('lines')
        print('Done')
    except:
        print('Nothing to delete')

    # make blank point and line feature classes
    print('Creating feature classes...')
    template = 'jsonoutput.csv'
    sr = arcpy.SpatialReference(4326)
    arcpy.CreateFeatureclass_management(working_path, "pts", "POINT", template, spatial_reference = sr)
    pointFC = 'pts'
    arcpy.CreateFeatureclass_management(working_path, "lines", "POLYLINE", template, spatial_reference = sr)
    polylineFC = 'lines'

    # loop through all records in .csv and write to FC's
    import pandas as pd
    import re
    df = []
    df = pd.read_csv('jsonoutput.csv')
    index_val = 0
    pt_index_val = 0
    line_index_val = 0
    while index_val < len(df):
        # Decide point or line
        feat_type = df.iloc[index_val].at['geography_type']
        # Do this for points
        if feat_type == 'Point':
            # Get coordinates for point
            pt_coord = df.iloc[index_val].at['geography_coordinates'][1:-1]
            pt_coord = re.split(",", pt_coord) # create list of x,y
            lat = float(pt_coord[1])
            lon = float(pt_coord[0])
            vertex = (lon, lat) # define point geometry

            # Write the coordinate list to the feature class as a point feature
            with arcpy.da.InsertCursor(pointFC, ('SHAPE@XY')) as cursor:
                cursor.insertRow((vertex,))

            # Get attribute values for point
            headers = list(df.columns[1:]) 
            column_values = {}
            for column in headers:
                column_values[column] =  df.iloc[index_val].at[column]

            sql_exp = f"OBJECTID = {pt_index_val+1}"
            # Use update cursor to add attribute data
            with arcpy.da.UpdateCursor(pointFC, field_names  = ('url',
                                                            'id',
                                                            'headline',
                                                            'status',
                                                            'created',
                                                            'updated',
                                                            'description',
                                                            'event_type',
                                                            'event_subtypes',
                                                            'severity',
                                                            'roads',
                                                            'areas',
                                                            'schedule_intervals',
                                                            'geography_type',
                                                            'geography_coordinates',), where_clause = sql_exp) as cursor2:
                for row in cursor2:
                    for column in headers:
                        for x in range(0,15):
                            row[x] =  list(column_values.values())[x] 
                    cursor2.updateRow(row)
            print(index_val, ": point", pt_index_val+1, "added")
            pt_index_val +=1
            index_val += 1
        # Do this for lines
        elif feat_type == 'LineString':
            # Get coordinates for line from CSV
            line_str = df.iloc[index_val].at['geography_coordinates'][1:-1] # set variable for list coordinates from csv
            line_list = re.split("(\[.+?\])", line_str) # split string into list
            for pt in line_list: # remove comma-only items from list
                if "[" not in pt:
                    line_list.remove(pt)
            new_line = [] # remove leading and trailing square brackets
            for pt in line_list: 
                new_line.append(pt.translate({91: '', 93:''}))
            new_line2 = [] # create list of lists (for each pt)
            for pt in new_line:
                new_line2.append(re.split(",", pt))

            # Create an empty list in which to make nice geometry for ArcGIS
            vertices = []

            # loop through the points and get each coordinate
            for pt in new_line2:
                lat = float(pt[1])
                lon = float(pt[0])

                # Put the coords into a tuple and add it to the list
                vertex = (lon,lat)
                vertices.append(vertex)

            # Write the coordinate list to the feature class as a polyline feature
            with arcpy.da.InsertCursor(polylineFC, ('SHAPE@')) as cursor:
                cursor.insertRow((vertices,))

            # Get attribute values for line
            headers = list(df.columns[1:]) 
            column_values = {}
            for column in headers:
                column_values[column] =  df.iloc[index_val].at[column]

            sql_exp = f"OBJECTID = {line_index_val+1}"

            # Use update cursor to add attribute data
            with arcpy.da.UpdateCursor(polylineFC, field_names  = ('url',
                                                            'id',
                                                            'headline',
                                                            'status',
                                                            'created',
                                                            'updated',
                                                            'description',
                                                            'event_type',
                                                            'event_subtypes',
                                                            'severity',
                                                            'roads',
                                                            'areas',
                                                            'schedule_intervals',
                                                            'geography_type',
                                                            'geography_coordinates',), where_clause = sql_exp) as cursor3:
                for row in cursor3:
                    for column in headers:
                        for x in range(0,14):
                            row[x] =  list(column_values.values())[x] 
                    cursor3.updateRow(row)
            print(index_val, ": line", line_index_val+1, "added")
            line_index_val +=1
            index_val += 1
    print('All features created successfully')
csv2gdb()

def exportmaps():
    print('EXPORT MAPS')
    print('Importing data...', end='')

    ### IMPORT UPDATED FEATURE CLASSES TO PROJECT ###
    import os
    import arcpy
    # set environments
    map_working_path = os.getcwd()+r"\DriveBC_prj"
    arcpy.env.workspace = map_working_path 
    arcpy.env.overwriteOutput = True
    sr = arcpy.SpatialReference(4326)
    with arcpy.EnvManager(outputCoordinateSystem=sr):
        pts = os.getcwd()+"\\driveBC.gdb\\pts"
        lines = os.getcwd()+"\\driveBC.gdb\\lines"
        ReclassLUT_csv = "ReclassLUT.csv"
        
        # Join to simplify symbology fields (pts)
        pts = arcpy.management.JoinField(in_data=pts, in_field="event_type", join_table=ReclassLUT_csv, join_field="pt_event_type", fields=["pt_event_type_simp"])[0]

        # Copy feature to overwrite existing feature class data source (pts)
        pts_import = "\\DriveBC_prj.gdb\\pts_import"
        arcpy.management.CopyFeatures(in_features=pts, out_feature_class=pts_import, config_keyword="", spatial_grid_1=None, spatial_grid_2=None, spatial_grid_3=None)

        # Join to simplify symbology fields (lines)
        lines = arcpy.management.JoinField(in_data=lines, in_field="event_type", join_table=ReclassLUT_csv, join_field="line_event_type", fields=["line_event_type_simp"])[0]

        # Copy feature to overwrite existing feature class data source (lines)
        lines_import = "\\DriveBC_prj.gdb\\lines_import"
        arcpy.management.CopyFeatures(in_features=lines, out_feature_class=lines_import, config_keyword="", spatial_grid_1=None, spatial_grid_2=None, spatial_grid_3=None)

        print("Done")
        
        ### EXPORT LAYOUTS ###
        print(f"Exporting maps to {os.getcwd()}"+r'\export')
        aprx = arcpy.mp.ArcGISProject(os.getcwd()+r'\DriveBC_prj\DriveBC_prj.aprx')
        lyt = aprx.listLayouts()[0]
        mf = lyt.listElements("MAPFRAME_ELEMENT")[0]
        if not os.path.exists('export'):
            os.makedirs('export')

        bkmks = mf.map.listBookmarks()
        for bkmk in bkmks:
            mf.zoomToBookmark(bkmk)
            lyt.name = bkmk.name
            lyt.exportToPDF(os.path.join('export', f"{bkmk.name}.pdf"))
            print(f"{bkmk.name}.pdf exported")
        print("All maps exported successfully.")

        # Save geodatabase
        import time
        saveit = input('Save ArcGIS project? (Y/N)')
        if saveit.lower() == 'y':
            aprx.saveACopy(os.path.join(os.getcwd()+r'\DriveBC_prj\DriveBC_prj_updated_'+time.strftime("%d%m%Y_%H%M", time.localtime())+'.aprx'))
            print('Project saved.')
        del aprx
exportmaps()
