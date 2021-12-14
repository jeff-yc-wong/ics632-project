# Parallelizing Astronomical Mosaic Workflow with DASK
A GitHub repo that stores our code for the ICS632 final project, Parallelizing Astronomical Mosaic Workflow with DASK.

## To pull the docker image hosted on Docker Hub
```
docker pull jeffycwong/ics632-project
```

## To run the docker image in shell mode, go inside the directory with the run_it.sh files and type:

Linux/MacOS:
```
docker run -it -v $(pwd):/home/user -p 8787:8787 jeffycwong/ics632-project
```

Windows (Powershell):
```
docker run -it -v ${pwd}:/home/user -p 8787:8787 jeffycwong/ics632-project
```

Windows (CMD):
```
docker run -it -v %cd%:/home/user -p 8787:8787 jeffycwong/ics632-project
```


## Running the run_it.sh Script:

Linux/MacOS:
```
docker run -it -v $(pwd):/home/user -p 8787:8787 jeffycwong/ics632-project bash run_it.sh
```

Windows (Powershell):
```
docker run -it -v ${pwd}:/home/user -p 8787:8787 jeffycwong/ics632-project bash run_it.sh
```

Windows (CMD):
```
docker run -it -v %cd%:/home/user -p 8787:8787 jeffycwong/ics632-project bash run_it.sh
```
