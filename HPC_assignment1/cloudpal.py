
from mpi4py import MPI
import time
import sys
start = time.clock()
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# The class is used to represent grid's attributes
class Grid():
    id=""
    xmin=0
    xmax=0
    ymin=0
    ymax=0
    count=0

    def __init__(self,id,xmin,xmax,ymin,ymax,count):
        self.id = id
        self.xmin = xmin
        self.xmax = xmax
        self.ymin = ymin
        self.ymax = ymax
        self.count = count

# a function to judge whether an instagram is located in a grid
def gridCount(line,grid_list):
	# the sub string 2 bit to 4 bit is "id" will be used as input data
    if line [2:4] == "id":
        subline = ""
        try:
        	# the last two bit is "," and "\n"
            subline = json.loads(line[:-2])
        except ValueError:
        	# the last bit is "\n"
            subline = json.loads(line[:-1])

        if 'doc' in subline.keys() and 'coordinates' in subline['doc'].keys() and 'coordinates' in subline['doc']['coordinates']:
            # some coordinates are empty
            if len(subline['doc']['coordinates']['coordinates'])==2:
                coor = subline['doc']['coordinates']['coordinates']

            for grid in grid_list:
                if coor[1]>= grid.xmin and coor[1] < grid.xmax and coor[0] >= grid.ymin and coor[0] < grid.ymax:
                    grid.count+=1
    return grid_list

grid_list=[]

# load the melb grid map to the program
import json
with open("melbGrid.json","r") as f:
    file = json.loads(f.read())

    for row in file['features']:
        temp = Grid(row['properties']['id'],row['properties']['xmin'],
                    row['properties']['xmax'],row['properties']['ymin'],
                    row['properties']['ymax'],0)
        grid_list.append(temp)
# load the instagram dataset
def process(grid_list):
    filename = sys.argv[1]
    file2 = open(filename,"r", encoding = "utf-8")
    count=0
    for line in file2:
        count+=1  
        # allocate the row of instagram to a specific process
        if rank == count%size:
            
            try:
                
                grid_list = gridCount(line,grid_list)
            except:
                continue
    # collect data from all cores     
    gather_grid_list = comm.gather(grid_list,root=0)

    if rank ==0:
        result_dict = {'A1':0,'A2':0,'A3':0,'A4':0,'B1':0,'B2':0,'B3':0,'B4':0,'C1':0,'C2':0,'C3':0,'C4':0,'C5':0,'D3':0,'D4':0,'D5':0}
        #statistic the final result
        for grid_list in gather_grid_list:
            for grid in grid_list:
                result_dict[grid.id]+=grid.count
        end = time.clock()
        print("Rank the Grid boxes\n")
        sortResult(result_dict)
        print("\nRank the rows\n")
        mergeRow(result_dict)
        print("\nRank the columns\n")
        mergeColumn(result_dict)
        print("\nexecution time: ",end-start)

# sort the final result
def sortResult(result_dict):
    sorted_result = sorted(result_dict.items(), key=lambda x: x[1],reverse=True)
    for item in sorted_result:
        print(item)

# get the result based on rows
def mergeRow(result_list):
    row_dict = {'A-Row':0,'B-Row':0,'C-Row':0,'D-Row':0}
    row_dict['A-Row'] = result_list['A1']+result_list['A2']+result_list['A3']+result_list['A4']
    row_dict['B-Row'] = result_list['B1']+result_list['B2']+result_list['B3']+result_list['B4']
    row_dict['C-Row'] = result_list['C1']+result_list['C2']+result_list['C3']+result_list['C4']+result_list['C5']
    row_dict['D-Row'] = result_list['D3']+result_list['D4']+result_list['D5']
    sortResult(row_dict)

# get the result based on cloumns
def mergeColumn(result_list):
    column_dict = {'Column1':0,'Column2':0,'Column3':0,'Column4':0,'Column5':0}
    column_dict['Column1'] = result_list['A1']+result_list['B1']+result_list['C1']
    column_dict['Column2'] = result_list['A2']+result_list['B2']+result_list['C2']
    column_dict['Column3'] = result_list['A3']+result_list['B3']+result_list['C3']+result_list['D3']
    column_dict['Column4'] = result_list['A4']+result_list['B4']+result_list['C4']+result_list['D4']
    column_dict['Column5'] = result_list['C5']+result_list['D5']
    sortResult(column_dict)


process(grid_list)
