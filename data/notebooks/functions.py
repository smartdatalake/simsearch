import numpy as np
import pandas as pd
from shapely.geometry import Point, box
from shapely import wkt
import geopandas as gpd
import folium
from folium.plugins import HeatMap
from folium import IFrame
from folium.plugins import MarkerCluster
import math
import json
import requests
import matplotlib
from matplotlib import pyplot as plt
from wordcloud import WordCloud
import statistics


def bbox(gdf):
    """Computes the bounding box of a GeoDataFrame (Adaptation from LOCI).

    Args:
        gdf (GeoDataFrame): A GeoDataFrame.

    Returns:
        A Polygon representing the bounding box enclosing all geometries in the GeoDataFrame.
    """

    # Exclude all NULL geometries from computation
    gdf_clean = gdf[~gdf.is_empty]
    minx, miny, maxx, maxy = gdf_clean.geometry.total_bounds
    if np.isnan(minx) or np.isnan(miny) or np.isnan(maxx) or np.isnan(maxy):
        return Point()
    return box(minx, miny, maxx, maxy)



def wkt_to_geometry(p):
    """
    Convert a WKT representation of a geometry (usually a point location) into a geometry. 
    
    Args:   
        p: a Well-Known Text representation of a geometry. 
        
    Returns:  
        A binary representation of the geometry to be used in a geodataframe. 
    """
    try:
        return wkt.loads(p)
    except Exception:
        return None

    
def changeDataType(col):
    """
    Change datatype of a pandas column, process only if the column dtype is object. 
    
    Args:
        col: a pandas Series representing a df column. 
        
    Returns:  
        A pandas Series representing a df column with the new data type (if changed). 
    """
    isWKT = False
    
    if pd.api.types.is_object_dtype(col):
 
        # try numeric
        try:
            if isinstance(col[0],float):
                col = col.astype(str).astype(float)  # float
            else:
                col = col.astype(str).astype(int)  # integer
        except:
            # try numeric
            try:
                col = pd.to_numeric(col.dropna().unique())
            except:
                # try date time
                try:
                    col = pd.to_datetime(col.dropna().unique())
                except:
                    try:
                        pd.to_timedelta(col.dropna().unique())
                    except:
                        try:
                            # try POINT locations in WKT representation
                            if 'POINT (' in col.any():
                                col = col.apply(wkt_to_geometry)
                                isWKT = True
                            else:   
                                # try array of strings (e.g., keywords)
                                col = col.str[1:-1].str.split(',')
                        except:    # Return original column intact
                            return col

    return col, isWKT




def flatten(results):
    """
    Custom function for flattening the JSON results obtained from a similarity search request.
    The flattened (geo)dataframe can be used for post-processing (e.g., map visualization, statistics, etc.)
    Args:   
        results: A JSON object that contains results for one combination of weights specified in a similarity search query.
    Returns:   
        A geodataframe with the flattened results for this combination of weights.
    """
    
    # Isolate specific properties (attribute name, its value for each result, and its similarity score) contained in the nested array
    dfAttr = pd.json_normalize(results, 'attributes', ['id'])
    dfAttrFlat = dfAttr.drop_duplicates(['id','name']).set_index(['id','name']).unstack()
    dfAttrFlat.columns = [f'{j}_{i}' for i, j in dfAttrFlat.columns]
    dfAttrFlat.reset_index()

    # Rest of the properties in the results
    dfRest = pd.json_normalize(results).loc[:, ['id','name','score','rank','exact']]

    # Merge with the flattened attribute properties
    # This provides all infomation about this batch of top-k results
    dfFinal = pd.merge(dfRest, dfAttrFlat, on=['id','id'])

    ## Attribute names
    cols = dfFinal.columns.tolist()
   
    # Create a copy for conversion to geodataframe
    pois = dfFinal.copy() 
    value_cols = [col for col in pois.columns if '_value' in col]

    # Change data types for spatial, numerical and keyword attributes
    col_wkt = None
    for column in value_cols:
        pois[column], isWKT = changeDataType(pois[column])
        # Identify the column containing spatial locations, if present
        if (isWKT):
            col_wkt = column

    # Create a geodataframe if a geometry column is present
    if (col_wkt != None):
        # Rename columns related to the spatial attribute
        s = col_wkt.find('_value')
        if (s > 0):
            pois.rename(columns={col_wkt[0:s]+'_score': 'geometry_score'}, inplace=True)
        
        # Transform geometries to WGS84 
        source_crs='EPSG:4326'
        target_crs='EPSG:4326'
        pois = gpd.GeoDataFrame(pois, crs=source_crs, geometry=pois[col_wkt]).to_crs(target_crs) 
        # Drop original attribute holding locations
        pois = pois.drop(columns=[col_wkt], axis=1)

    # Resulting flattened (geo)dataframe
    return pois



def filter_dict(origDict, threshold):
    """
    Iterates over all numerical values in a dictionary and return those above the given threshold.
    
    Args:
        origDict:  A dictionary of items.
        threshold: A numerical value to be used for filtering the input dictionary.
    
    Returns:
        The filtered dictionary.
    """
    
    newDict = dict()   
    for (key, value) in origDict.items():
       # Check if value matches the condition and then add pair to new dictionary
       if value >= threshold:
            newDict[key] = value

    return newDict



def filter_dict_median(origDict):
    """
    Iterates over all numerical values in a dictionary and return those above the median.
    
    Args:
        origDict:  A dictionary of items.
    
    Returns:
        The filtered dictionary.
    """   
    
    # Estimate threshold value from the median of the input numerical values
    threshold = statistics.median(list(origDict.values()))
    newDict = dict()   
    for (key, value) in origDict.items():
       # Check if value matches the condition and then add pair to new dictionary
       if value >= threshold:
            newDict[key] = value

    return newDict



def generate_color(w):
    """
    Generate a color using the weight values of the given array.
    
    Args:
        w:  A combination of weights specified in a similarity search request.
    
    Returns:
        The generated color value.
    """
    
    r = round(w[0] * 255) if w[0] is not None else 0
    g = round(w[1] * 255) if w[1] is not None else 0
    b = round(w[2] * 255) if w[2] is not None else 0
    color = '#{:02x}{:02x}{:02x}'.format(r, g, b)
    
    return color


def filterNaN(data):
    """
    Filter null values in order to get meaningful visualizations.
    
    Args:
        data: a pandas Series representing a df column. 
        
    Returns:
        The Series with the null values removed.
    """
    return list(data[~np.isnan(data)])



def results_pairwise(resultsA, resultsB):
    """
    Prepares lists of identifiers and scores for two batches of results.
    To be used in comparing correlation between the two batches.
    
    Args:
        resultsA: One batch of results.
        resultsB: Another batch or results.
    
    Returns:
        The two batches reorganized as lists of identifiers and scores.
    """
    
    # Keys (identifiers)
    keysA = resultsA['id'].to_list()
    keysB = resultsB['id'].to_list()

    # Values (scores)
    valuesA = resultsA['score'].to_list()
    valuesB = resultsB['score'].to_list()

    # All unique keys
    keys = list(set(keysA)) + list(set(keysB) - set(keysA))

    # Prepare new lists with all keys
    A = pd.DataFrame(np.zeros((1,len(keys))))
    B = pd.DataFrame(np.zeros((1,len(keys))))

    # Populate first list with values from the first batch
    for key in keys:
        if key in keysA:
            A[keys.index(key)]=valuesA[keysA.index(key)]
    #        print(arrayA[keys.index(key)])

    # Populate second list with values from the second batch
    for key in keys:
        if key in keysB:
            B[keys.index(key)]=valuesB[keysB.index(key)]
    #        print(arrayA[keys.index(key)])
    
    return A, B



def map_points(pois, tiles='OpenStreetMap', width='100%', height='100%', show_bbox=False):

    """Returns a Folium Map displaying the provided points. Map center and zoom level are set automatically.
    (Adapted from LOCI)

    Args:
         pois (GeoDataFrame): A GeoDataFrame containing the POIs to be displayed.
         tiles (string): The tiles to use for the map (default: `OpenStreetMap`).
         width (integer or percentage): Width of the map in pixels or percentage (default: 100%).
         height (integer or percentage): Height of the map in pixels or percentage (default: 100%).
         show_bbox (bool): Whether to show the bounding box of the GeoDataFrame (default: False).

    Returns:
        A Folium Map object displaying the given POIs.
    """
        
    # Set the crs to WGS84
    if pois.crs != 'epsg:4326':
        pois = pois.to_crs('epsg:4326')

    # Automatically center the map at the center of the bounding box enclosing the POIs.
    bb = bbox(pois)
    if bb.is_empty:
        return None
    map_center = [bb.centroid.y, bb.centroid.x]

    # Initialize the map
    m = folium.Map(location=map_center, tiles=tiles, width=width, height=height)

    # Automatically set the zoom level
    m.fit_bounds(([bb.bounds[1], bb.bounds[0]], [bb.bounds[3], bb.bounds[2]]))

    # Columns containing scores per attribute
    score_cols = [col for col in pois.columns if '_score' in col]
    # Column containing the entity name
    name_col = 'name'
    
    # Create chart plots in the background
    # Change to a backend that does not display to the user in order to avoid showing plots when creating them
    plt_backend=matplotlib.get_backend()
    matplotlib.use('Agg')
    
    # Add pois to a marker cluster
    coords, popups = [], []
    for idx, row in pois.iterrows():
        coords.append([row.geometry.y, row.geometry.x])
        label = str(row['id']) + '<br>' + str(row['name'])
        iframe = IFrame(html=label, width=200, height=80)
        popups.append(folium.Popup(iframe, min_width=100, max_width=200, parse_html=True))   
        
    poi_layer = folium.FeatureGroup(name='pois')
    poi_layer.add_child(MarkerCluster(locations=coords, popups=popups))
    m.add_child(poi_layer)

    # Restore the native backend for plots
    matplotlib.use(plt_backend)
  
    if show_bbox:
        folium.GeoJson(bb).add_to(m)

    return m


   
# Generates a Folium Map object displaying a weighted heatmap generated from the POIs (Adaptation from LOCI)
def weighted_heatmap(pois, tiles='OpenStreetMap', width='100%', height='100%', radius=10):

    """Generates a Folium Map object displaying a weighted heatmap generated from the POIs (Adaptation from LOCI)

    Args:
         pois (GeoDataFrame): A GeoDataFrame containing the POIs to be displayed.
         tiles (string): The tiles to use for the map (default: `OpenStreetMap`).
         width (integer or percentage): Width of the map in pixels or percentage (default: 100%).
         height (integer or percentage): Height of the map in pixels or percentage (default: 100%).
         radius (float): Radius of each point of the heatmap (default: 10).

    Returns:
        A Folium Map object displaying the heatmap generated from the POIs.
    """
    
    # Set the crs to WGS84
    if pois.crs != 'epsg:4326':
        pois = pois.to_crs('epsg:4326')

    # Automatically center the map at the center of the gdf's bounding box
    bb = bbox(pois)
    if bb.is_empty:
        return None
    map_center = [bb.centroid.y, bb.centroid.x]
    # Create the map
    heat_map = folium.Map(location=map_center, tiles=tiles, width=width, height=height)
    # Automatically set zoom level
    heat_map.fit_bounds(([bb.bounds[1], bb.bounds[0]], [bb.bounds[3], bb.bounds[2]]))

    # Make list of lists for the values
    heat_data = [[row['geometry'].y, row['geometry'].x, row['score']] for index, row in pois[~pois.is_empty].iterrows()]

    # Plot it on the map
    HeatMap(heat_data, radius=radius).add_to(heat_map)

    return heat_map


def frequency(gdf, col_kwds='kwds', normalized=False):
    """Computes the frequency of keywords in the provided GeoDataFrame (function adapted from LOCI).

    Args:
        gdf (GeoDataFrame): A GeoDataFrame with a keywords column.
        col_kwds (string) : The column containing the list of keywords (default: `kwds`).
        normalized (bool): If True, the returned frequencies are normalized in [0,1]
            by dividing with the number of rows in `gdf` (default: False).

    Returns:
        A dictionary containing for each keyword the number of rows it appears in.
    """

    kwds_ser = gdf[col_kwds]

    kwds_freq_dict = dict()
    for (index, kwds) in kwds_ser.iteritems():
        for kwd in kwds:
            if kwd in kwds_freq_dict:
                kwds_freq_dict[kwd] += 1
            else:
                kwds_freq_dict[kwd] = 1

    num_of_records = kwds_ser.size

    if normalized:
        for(kwd, freq) in kwds_freq_dict.items():
            kwds_freq_dict[kwd] = freq / num_of_records

    return kwds_freq_dict



def barchart(data, orientation='Vertical', x_axis_label='', y_axis_label='', plot_title='', bar_width=0.5,
             plot_width=15, plot_height=5, top_k=10):
    """Plots a bar chart with the given data.
    (Function adapted from LOCI)

    Args:
        data (dict): The data to plot.
        orientation (string): The orientation of the bars in the plot (`Vertical` or `Horizontal`; default: `Vertical`).
        x_axis_label (string): Label of x axis.
        y_axis_label (string): Label of y axis.
        plot_title (string): Title of the plot.
        bar_width (scalar): The width of the bars (default: 0.5).
        plot_width (scalar): The width of the plot (default: 15).
        plot_height (scalar): The height of the plot (default: 5).
        top_k (integer): Top k results (if -1, show all; default: 10).

    Returns:
        A Matplotlib plot displaying the bar chart.
    """
    
    sort_order = True
    if orientation != 'Vertical':
        sort_order = False

    sorted_by_value = sorted(data.items(), key=lambda kv: kv[1], reverse=sort_order)

    if top_k != -1:
        if orientation != 'Vertical':
            sorted_by_value = sorted_by_value[-top_k:]
        else:
            sorted_by_value = sorted_by_value[0:top_k]

    (objects, frequency) = map(list, zip(*sorted_by_value))
    y_pos = np.arange(len(objects))
    
    fig, ax = plt.subplots(figsize=(plot_width,plot_height))
    
    if orientation == 'Vertical':
        ax.bar(y_pos, frequency, width=bar_width, align='center', alpha=0.5)
        plt.xticks([])
        for i, (name, height) in enumerate(zip(objects, frequency)):
            ax.text(i, height, ' ' + name, ha='center', va='center')
    else:
        ax.barh(y=y_pos, width=frequency, align='center', alpha=0.5)
        plt.yticks([])
        for i, (name, height) in enumerate(zip(objects, frequency)):
            ax.text(height, i, ' ' + name, ha='center', va='center')

    plt.xlabel(x_axis_label)
    plt.ylabel(y_axis_label)
    
    plt.title(plot_title)

    return plt


def plot_wordcloud(pois, col_kwds='kwds', bg_color='black', width=400, height=200):
    """Generates and plots a word cloud from the keywords of the given POIs.

     Args:
        pois (GeoDataFrame): The POIs from which the keywords will be used to generate the word cloud.
        col_kwds (string) : The column containing the list of keywords (default: `kwds`).
        bg_color (string): The background color to use for the plot (default: black).
        width (int): The width of the plot.
        height (int): The height of the plot.
    """

    # Compute keyword frequences
    kf = frequency(pois, col_kwds)

    # Generate the word cloud
    wordcloud = WordCloud(background_color=bg_color, width=width, height=height).generate_from_frequencies(kf)

    # Show plot
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.show()
    
