# -*- coding: utf-8 -*-

"""
Production Mapper 

Michael Troyer
michael.troyer@usda.gov
"""


import datetime
import os
import traceback

from collections import defaultdict

import arcpy


arcpy.env.addOutputsToMap = False
arcpy.env.overwriteOutput = True


##---Functions-------------------------------------------------------------------------------------

def build_where_clause(table, field, valueList):
    """
    Takes a list of values and constructs a SQL WHERE
    clause to select those values within a given field and table.
    """
    # Add DBMS-specific field delimiters
    fieldDelimited = arcpy.AddFieldDelimiters(arcpy.Describe(table).path, field)
    # Determine field type
    fieldType = arcpy.ListFields(table, field)[0].type
    # Add single-quotes for string field values
    if str(fieldType) == 'String':
        valueList = ["'%s'" % value for value in valueList]
    # Format WHERE clause in the form of an IN statement
    whereClause = "%s IN(%s)" % (fieldDelimited, ', '.join(map(str, valueList)))
    return whereClause


def intersect_and_get_attributes(source_layer, intersect_layer, intersect_field):
    arcpy.SelectLayerByLocation_management(intersect_layer, 'INTERSECT', source_layer)
    if not arcpy.Describe(intersect_layer).FIDSet.split(';'):
        return []
    with arcpy.da.SearchCursor(intersect_layer, intersect_field) as cur:
        values = [row[0] for row in cur]
    arcpy.SelectLayerByAttribute_management(intersect_layer, "CLEAR_SELECTION")
    return values


class Toolbox(object):   
    def __init__(self):
        self.label = "Production Mapper"
        self.alias = "Production_Mapper"
        
        # List of tool classes associated with this toolbox
        self.tools = [ProductionMapper]


class ProductionMapper(object):
    def __init__(self):
        self.label = "ProductionMapper"
        self.description = ""
        self.canRunInBackground = True
        
    def getParameterInfo(self):
        
        input_fc=arcpy.Parameter(
            displayName="Input Feature Class",
            name="Input Feature Class",
            datatype="Feature Class",
            parameterType="Required",
            direction="Input",
            )
        project_id=arcpy.Parameter(
            displayName="Project ID",
            name="Project ID",
            datatype="String",
            parameterType="Optional",
            )
        title=arcpy.Parameter(
            displayName="Project Title",
            name="Project Title",
            datatype="String",
            parameterType="Optional",
            )
        author=arcpy.Parameter(
            displayName="Author",
            name="Author",
            datatype="String",
            parameterType="Optional",
            )
        template=arcpy.Parameter(
            displayName="Select Map Template",
            name="Select Map Template",
            datatype="DEMapDocument",
            parameterType="Required",          
            direction="Input",
            )
        output_mxd=arcpy.Parameter(
            displayName="Output Map Document",
            name="Output Map Document",
            datatype="DEMapDocument",
            parameterType="Required",
            direction="Output",
            )
                
        return [input_fc, project_id, title, author, template, output_mxd]


    def isLicensed(self):
        return True


    def updateParameters(self, params):
        params[0].filter.list = ["Polygon"]
        return


    def updateMessages(self, params):                               
        return


    def execute(self, params, messages):    
        input_fc, project_id, title, author, template, output_mxd = params

        try:
            # for param in params:
            #     arcpy.AddMessage('{} [Value: {}]'.format(param.name, param.value))

            layer = arcpy.MakeFeatureLayer_management(input_fc.value, "in_memory\\tmp")
            mxd = arcpy.mapping.MapDocument(template.valueAsText)
            df = arcpy.mapping.ListDataFrames(mxd)[0]

            database = r'.\Production_Mapper.gdb'

            counties_layer = arcpy.MakeFeatureLayer_management(os.path.join(database, 'Counties'), r'in_memory\Counties')
            quads_layer = arcpy.MakeFeatureLayer_management(os.path.join(database, 'Quad_Index_24k'), r'in_memory\Quads')
            plss_layer = arcpy.MakeFeatureLayer_management(os.path.join(database, 'PLSS_FirstDivision'), r'in_memory\PLSS')
            utm_zone_layer = arcpy.MakeFeatureLayer_management(os.path.join(database, 'UTM_Zones'), r'in_memory\UTM_Zone')

            counties = intersect_and_get_attributes(layer, counties_layer, 'LABEL')
            plss = intersect_and_get_attributes(layer, plss_layer, 'FRSTDIVID')
            quads = intersect_and_get_attributes(layer, quads_layer, 'QUADNAME')
            utm_zone = intersect_and_get_attributes(layer, utm_zone_layer, 'UTM_Zone')

            # Counties
            county_text = 'County(s):\n{}'.format(', '.join(counties))
            arcpy.AddMessage(county_text)
            
            # Quads
            quad_text = "7.5' Quad(s):\n{}".format(', '.join(quads))
            arcpy.AddMessage(quad_text)

            # PLSS
            plss_data = defaultdict(list)
            for row in plss:
                pm = int(row[2:4])
                tw = row[5:7] + row[8]
                rg = row[10:12] + row[13]
                sn = int(row[17:19])
                plss_data[(pm, tw, rg)].append(sn)
            plss_text = '\n'.join(
                [
                    'PM {} | Twn {} | Rng {} \nSections: {}'.format(
                        pm, tw, rg, ', '.join([str(s) for s in sorted(secs)])
                        )
                    for (pm, tw, rg), secs in plss_data.items()
                ]
            )
            arcpy.AddMessage(plss_text)
            
            # UTM Coordinates
            dissolve = arcpy.Dissolve_management(layer, r'in_memory\dissolve')
            dissolve_layer = arcpy.MakeFeatureLayer_management(dissolve, r'in_memory\dissolve_layer')
            with arcpy.da.SearchCursor(dissolve_layer, "SHAPE@XY") as cur:
                for pt, in cur:
                    mX, mY = pt
            utm_e = round(mX, 0)
            utm_n = round(mY, 0)

            utm_text = '{}N | {} mN | {} mE'.format(max(utm_zone), utm_n, utm_e)
            arcpy.AddMessage(utm_text)

            # Date
            now = datetime.datetime.now()
            date_text = r'Map Date: {}/{}/{}'.format(now.month, now.day, now.year)
            arcpy.AddMessage(date_text)

            # Get and update the layout elements
            layout_elements = {le.name: le for le in arcpy.mapping.ListLayoutElements(mxd)}

            for field, update in {
                'County': county_text,
                'Quad': quad_text,
                'PLSS': plss_text,
                'UTM': utm_text,
                'Date': date_text,
                'Project ID': project_id.valueAsText,
                'Title': title.valueAsText,
                'Author': author.valueAsText,
            }.items():
                if update:
                    try: 
                        layout_elements[field].text = update
                    except KeyError:
                        pass

            # Update map and save output                   
            arcpy.mapping.AddLayer(df, arcpy.mapping.Layer(input_fc.valueAsText), "TOP")
            df.extent = arcpy.Describe(layer).extent

            df.scale = round(df.scale * 1.25, -2)

            arcpy.RefreshActiveView
            arcpy.RefreshTOC           

            output = output_mxd.valueAsText
            if not output.endswith('.mxd'):
                output += '.mxd'
            mxd.saveACopy(output)
            
            # Clean up
            for item in (layer, counties_layer, quads_layer, plss_layer, utm_zone_layer, dissolve):
                try:
                    arcpy.Delete_management(item)
                except:
                    pass
        except:
            arcpy.AddError(str(traceback.format_exc()))      

        return                      
