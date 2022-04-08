'''
# important code to mark
source ~/.bash_profile
conda deactivate
mpiexec -n 4 python -m mpi4py mpitest.py
'''

from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()

if comm_rank ==0:
    with open('tinyTwitter.json','r',encoding='utf-8') as fp:
        fp.readline()
        file = list(fp)

local_data = comm.scatter(file, root=0)


print(local_data)

# with open('tinyTwitter.json','r',encoding='utf-8') as fp:
#     print(type(list(fp)))



# # 1
# comm = MPI.COMM_WORLD
# rank = comm.Get_rank()
#
# if rank == 0:
#     # in real code, this section might
#     # read in data parameters from a file
#     numData = 10
#     comm.send(numData, dest=1)
#
#     # data = np.linspace(0.0, 3.14, numData)
#     # comm.Send(data, dest=1)
#
# elif rank == 1:
#
#     numData = comm.recv(source=0)
#     print('Number of data to receive: ', numData)
#
#     # data = np.empty(numData, dtype='d')  # allocate space to receive the array
#     # comm.Recv(data, source=0)
#
#     # print('data received: ', data)




# # 3
#
# """
# Demonstrates the usage of reduce, Reduce.
#
# Run this with 4 processes like:
# $ mpiexec -n 4 python reduce.py
# """
#
# import numpy as np
# from mpi4py import MPI
#
#
# comm = MPI.COMM_WORLD
# rank = comm.Get_rank()
# size = comm.Get_size()
#
# # ------------------------------------------------------------------------------
# # reduce generic object from each process to root by using reduce
# if rank == 0:
#     send_obj = 0.5
# elif rank == 1:
#     send_obj = 2.5
# elif rank == 2:
#     send_obj = 3.5
# else:
#     send_obj = 1.5
#
# # reduce by SUM: 0.5 + 2.5 + 3.5 + 1.5 = 8.0
# # 默认root=0
# recv_obj = comm.reduce(send_obj, op=MPI.SUM, root=1)
# print('reduce by SUM: rank %d has %s' % (rank, recv_obj))
# # reduce by MAX: max(0.5, 2.5, 3.5, 1.5) = 3.5
# recv_obj = comm.reduce(send_obj, op=MPI.MAX, root=2)
# print('reduce by MAX: rank %d has %s\n' % (rank, recv_obj))


# # ------------------------------------------------------------------------------
# # reduce numpy arrays from each process to root by using Reduce
# send_buf = np.array([0, 1], dtype='i') + 2 * rank
# if rank == 2:
#     recv_buf = np.empty(2, dtype='i')
# else:
#     recv_buf = None
#
# # Reduce by SUM: [0, 1] + [2, 3] + [4, 5] + [6, 7] = [12, 16]
# comm.Reduce(send_buf, recv_buf, op=MPI.SUM, root=2)
# print('Reduce by SUM: rank %d has %s' % (rank, recv_buf))
#
#
# # ------------------------------------------------------------------------------
# # reduce numpy arrays from each process to root by using Reduce with MPI.IN_PLACE
# send_buf = np.array([0, 1], dtype='i') + 2 * rank
# if rank == 2:
#     # initialize recv_buf with [-1, -1]
#     recv_buf = np.zeros(2, dtype='i') - 1
# else:
#     recv_buf = None
#
# # Reduce by SUM with MPI.IN_PLACE: [0, 1] + [2, 3] + [-1, -1] + [6, 7] = [7, 10]
# if rank == 2:
#     comm.Reduce(MPI.IN_PLACE, recv_buf, op=MPI.SUM, root=2)
# else:
#     comm.Reduce(send_buf, recv_buf, op=MPI.SUM, root=2)
# print('Reduce by SUM with MPI.IN_PLACE: rank %d has %s' % (rank, recv_buf))



#mpimp.py

#mpimp.py



# if comm_rank == 0:
#     data = [1,2,3]
#     comm.bcast(data, root=0)
# else:
#     # test_data = [1,2]
#     test_data = comm.bcast(None, root=0)
#     # print("Process %d receive"%comm_rank,test_data)
#
# # if语句外的所有操作会影响所有core
# # 如果定义变量则所有core自动增加这个变量
# test_data=[1,2]
# print("Process %d receive"%comm_rank,data)


# 在if语句外赋值之前每个core中都必须定义相同的变量名字
# if comm_rank == 0:
#     data = {'key1': [1, 2, 3],
#             'key2': ('abc', 'xyz')}
# elif comm_rank==1:
#     data=['test out']
# else:
#     data = None
#
# # bcast把指定root中定义的数据广播给所有core
# data = comm.bcast(data, root=0)
# # 如果直接广播某一个值，并定义变量，则
# testdata = comm.bcast('test',root=0)
# print('Rank: ', comm_rank, ', testdata: ', testdata,'data',data)

# reduce

# k = (1.0 if comm_rank%2 == 0 else -1.0)/(2*comm_rank +1)
# # reduce指定一个root输出
# data = comm.reduce(k, root=0,op=MPI.SUM)
# # 只有那一个core的数据会改变
# # 只有root 0 中的数据变化了
# print('test %d'%comm_rank,data)
# if comm_rank == 0:
#     pi = data*4
#     print("PI = %.6f"%pi)
