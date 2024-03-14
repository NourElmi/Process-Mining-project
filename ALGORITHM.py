from neo4j import GraphDatabase
import sys

# Set the maximum digits for integer string conversion
sys.set_int_max_str_digits(10000040)


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
    
def execute_query(query, parameters=None):   # Function to execute Cypher queries
    with driver.session(database="bpic19") as session:
        result = session.run(query, parameters)
        result_list = list(result)           # Convert the result to a list to fetch all records
        return result_list
############################################
    
# Algorithm 1: Find Maximal Significant DF Paths
def find_maximal_significant_df_paths(ECKG, frequency_threshold, significance_threshold):
 
    significant_paths = []         # Start with an empty list of significant paths.
    for i in range(len(ECKG)):     #For each edge(relation) in ECKG 
        # print("HELLO ")
        path = [ECKG[i]]           # Initialize a path with that edge.
        expand_and_check_maximal_path(path, ECKG, frequency_threshold, significance_threshold, significant_paths)

    return significant_paths
############################################

# Algorithm 2: Expand And Check Maximal Path

def expand_and_check_maximal_path(path, ECKG, frequency_threshold, significance_threshold, significant_paths):
    is_maximal = True

    for i in range(len(ECKG)):
        # Check if the edge is not already in the path
        if ECKG[i] not in path:
            new_path = path + [ECKG[i]]
            if (
                absolute_frequency(new_path) >= frequency_threshold
                and relative_frequency(new_path, new_path[0][1]) >= significance_threshold
            ):
                is_maximal = False
                expand_and_check_maximal_path(new_path, ECKG, frequency_threshold, significance_threshold, significant_paths)

    if is_maximal and path not in significant_paths:
        significant_paths.append(path)
#        print(significant_paths)

############################################

#Calculate Absolute frequency of a path
    
def absolute_frequency(path):
    
    frequencyRelation = float('inf')
    frequencyPath = 1
    for i in range(len(path)):# Iterate over each directly-follows relation in the path
       
        frequency_of_relation_In_ECKG = path[i][4]   # Get the frequency of the Current relation in ECKG
        frequencyRelation = min(frequencyRelation, frequency_of_relation_In_ECKG)
        #frequencyPath = frequencyPath * frequencyRelation

    return frequencyPath    # Return the calculated absolute frequency of the path in ECKG
############################################

# Algorithm 4: Calculate Relative Frequency of a Path
def relative_frequency(path, starting_class):
    
    total_count = 1
    pathCount = 0
    for i in range(len(path)):
        TotalOfEvents = get_Events_In_StartingClass(path[i][1])
        #for j in range(len(TotalOfEvents)):
        total_count = total_count * len(TotalOfEvents)
    TotalOfEvents = get_Events_In_StartingClass(path[len(path)-1][2])
    #for k in range(len(TotalOfEvents)):
    total_count = total_count * len(TotalOfEvents)

    allEvents_In_StartingClass = get_Events_In_StartingClass(starting_class)
    for i in range(len(allEvents_In_StartingClass)):
        pathCount += Count_Path_In_EKG(allEvents_In_StartingClass[i])
    
    if total_count > 0 :
        relative_frequency = pathCount / total_count

    return relative_frequency

# DEFInition of function to get data from neo4j 


#1- get all the events that are observed by classid

def get_Events_In_StartingClass(Class_ID):
    # Cypher query to retrieve both CORR and DF relationships
    query = """
    MATCH (e: Event)-[r: OBSERVED]-(StartingClass :Class)
    where id(StartingClass)= $idStartingClass
    RETURN id(e)
    Limit 5
    
    """
    parameters = {"idStartingClass": Class_ID}
    allEventsInStartingClass_Result = execute_query(query, parameters)
    List_IDs_of_All_Events = []
    for record in allEventsInStartingClass_Result:
        ID_Event = record.get("id(e)")
        List_IDs_of_All_Events.append(ID_Event)

    return List_IDs_of_All_Events

#2- Count_Path_In_EKG: Get total count of Paths that start with a specific Event

def Count_Path_In_EKG(ID_Event_In_Start_Class):
    # Cypher query to retrieve both CORR and DF relationships
    query = """
    MATCH (event1:Event)-[relDF:DF]->(event2:Event)
    WHERE id(event1) = $start_Event_ID
    RETURN COUNT(relDF) AS dfCount 
    Limit 5;
    """
    parameters = {"start_Event_ID": ID_Event_In_Start_Class}
    Count_Path_In_EKG_Starting_With_this_Event = execute_query(query, parameters)
    for record in Count_Path_In_EKG_Starting_With_this_Event:
        Count_Paths_that_start_with_the_Event = record.get("dfCount")
    
    return Count_Paths_that_start_with_the_Event


#3- ECKG FUNCTION: Get Event Class Knowledge Graph (ECKG) from Neo4j
def get_eckg_from_neo4j():
    query = """
    MATCH p=()-[relDF_BETWEEN_CLASSES:DF_Classes]->()
    RETURN id(startNode(relDF_BETWEEN_CLASSES)) as startClass, ID(relDF_BETWEEN_CLASSES) as idRelationBetweenThe2Classes, id(endNode(relDF_BETWEEN_CLASSES)) as endClass, relDF_BETWEEN_CLASSES.DF_count AS df_countBetween_2Events, relDF_BETWEEN_CLASSES.df_ids as DFs_IDs_Between_2Events
    LIMIT 7
    """
    eckg_result = execute_query(query)
    # print("This is my_ECKG", eckg_result)
    dfs_count_and_ids_Df_Total_List = []

    for record in eckg_result:
        id_rel_between_2classes = record.get("idRelationBetweenThe2Classes")
        startClass = record.get("startClass")
        endClass = record.get("endClass")
        DFs_IDs_Between_2Events = record.get("DFs_IDs_Between_2Events")
        Count_DF_between_Events = record.get("df_countBetween_2Events")
        df_count_and_id_Df_List = [id_rel_between_2classes, startClass, endClass, DFs_IDs_Between_2Events, Count_DF_between_Events]
        dfs_count_and_ids_Df_Total_List.append(df_count_and_id_Df_List)

    return dfs_count_and_ids_Df_Total_List



#At the end this to run the code

def run_analysis(driver, frequency_threshold, significance_threshold, output_file="result_pathsf.txt"):
    # Create an instance of EKG and ECKG
    ECKG = get_eckg_from_neo4j()
    # print("ECCCCCCCCC", ECKG)
    print("HELLO11")
    # Run the algorithm
    result_paths = find_maximal_significant_df_paths(ECKG, frequency_threshold, significance_threshold)
    # print("the resutls of find_maximal_significant_df_paths", result_paths)

    # Write the result paths to a file
    write_result_paths_to_file(result_paths, output_file)

    # Close the Neo4j driver when done
    driver.close()

# Function to write result paths to a file
def write_result_paths_to_file(result_paths, output_file):
    with open(output_file, 'w') as file:
        for path in result_paths:
            file.write(str(path) + '\n')

# Example call for a specific database using the existing driver
run_analysis(driver, frequency_threshold=1, significance_threshold=0.1)