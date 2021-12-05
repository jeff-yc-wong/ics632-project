from dask.distributed import Client
import time

# Create Spark client


class Worker:
    def some_func(self, *args):
        print("Hello World")
        return 10


if __name__ == '__main__':   
    client = Client()
    
    task = Worker()


    # Submit an invocation of the function to dask, which returns future
    f = client.submit(task.some_func)

    print("Sleeping for 10 seconds")
    time.sleep(10)
    # print the future
    print(f)
    
    # Print result of computation, which blocks until future is in finished state
    print(f.result())

            
