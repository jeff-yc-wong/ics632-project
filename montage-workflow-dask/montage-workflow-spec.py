#!/usr/bin/env python3

import os
import argparse
import re
import subprocess
import sys
import time

from astropy.io import ascii
from dask.distributed import Client, get_client

verbose = False


'''
Task class
'''
class Task:
    def __init__(self, executable):
        self.executable = executable
        self.inputfiles = []
        self.outputfiles = []
        self.arguments = []

    '''
    Method to add input files to the task
    '''
    def add_inputs(self, *args):
        for arg in args:
            self.inputfiles.append(arg) 

    '''
    Method to add output files to the task
    '''
    def add_outputs(self, *args, stage_out):
        for arg in args:
            self.outputfiles.append(arg) 

    '''
    Method to add command-line arguments to the task
    '''
    def add_args(self, *args):
        for arg in args:
            self.arguments.append(arg)

    '''
    Method to print the task
    '''
    def print(self):
        print("  * Task for executable " + self.executable)
        print("    - command-line: " + self.executable + ' '.join(self.arguments))
        print("    - input files: ")
        for f in self.inputfiles:
            print("        - " + f)
        print("    - output files: ")
        for f in self.outputfiles:
            print("        - " + f)
    
    '''
    Method to run the task
    '''
    def run(self, *args):
        global verbose

        sys.stderr.write("Current Working Directory: %s \n" % os.getcwd())
        sys.stderr.write("Running a " + self.executable + " task with input files {" + ', '.join(self.inputfiles) + "} " + 
        "and output files {" + ', '.join(self.outputfiles) + "}\n")

        cmd = self.executable + " " + ' '.join(self.arguments)

        if verbose:
            redirect = None
            sys.stderr.write('\tRunning sub command: ' + cmd + "\n")
        else:
            redirect = subprocess.DEVNULL

        start = time.time()
        os.chdir("/home/user/data/")
        if subprocess.call(cmd, shell=True, stderr=redirect, stdout=redirect) != 0:
            sys.stderr.write('\tCommand ' + cmd + ' failed!')
            sys.exit(1)
        end = time.time()
        os.chdir("/home/user")
        sys.stderr.write("  [executed in " + str("{:.2f}".format(end - start)) + " seconds]\n")



'''
Workflow class
'''
class Workflow:
    def __init__(self, name):
        self.name = name    
        self.tasks = []
        self.files_to_download = {}

    '''
    Method to add a task to the workflow
    '''
    def add_tasks(self, *tasks):
        for task in tasks:
            self.tasks.append(task)

    ''' 
    Method to print the workflow, if needed 
    '''
    def print(self, path):
        print("Workflow " + self.name + " with " + str(len(self.tasks)) + " tasks")
        for task in self.tasks:
            task.print()

    ''' 
    Method to add a file that will have to be downloaded from IPAC
    '''
    def add_file_to_download(self, location, f, url):
        if location != "ipac":
            return # local file
        self.files_to_download[f] = url
        return

    '''
    Method to download the necessary input files from IPAC
    '''
    def download_all_input_files(self):
        global verbose

        sys.stderr.write("Downloading FITS files from IPAC...\n");
        if verbose:
            redirect = None
        else:
            redirect = subprocess.DEVNULL

        os.chdir("./data/")
        count = 0
        for f in self.files_to_download:
            if not os.path.isfile(f):
                count += 1
                sys.stderr.write("Downloading input file " + f + "\n")
                cmd = "wget \"" + self.files_to_download[f] + "\" -O " + f
                start = time.time()
                if verbose:
                    sys.stderr.write('\tRunning sub command: ' + cmd + "\n")
                if subprocess.call(cmd, shell=True, stderr=redirect, stdout=redirect) != 0:
                    sys.stderr.write('\tCommand ' + cmd + ' failed!')
                    sys.exit(1)
                end = time.time()
                sys.stderr.write("  [downloaded in " + str("{:.2f}".format(end - start)) + " seconds]\n")
        os.chdir("../")

        sys.stderr.write("Downloaded " + str(count) + " files.\n")

    '''
    Method to execute the workflow sequentially
    '''
    def run(self):

        sys.stderr.write("Add task to DASK client...\n")

        # Make a dictionaty of all the tasks' output files, some of which
        # serve as input to other tasks. The dictionary key is the file name,
        # and the value is true if the file has been produced already, false 
        # otherwise. Note that this dictionary doesn't store entries to
        # the worklow's input file
        all_output_files = {}
        for task in self.tasks:
            for f in task.outputfiles:
                all_output_files[f] = False
    
        ready_list = []
        client = get_client()
        all_futures = []

        # Loop until tasks remain (yes, this loop "destroys" the workflow)
        while len(self.tasks) > 0:

            # Find list of tasks that are ready
            ready_tasks = []
            for task in self.tasks:
                ready = True
                # Check if all the task's inputfiles are available
                for f in task.inputfiles:
                    # if the input file is the output of another task
                    # check whether it has already been produced or not
                    if f in all_output_files:
                        if all_output_files[f] == False:
                            ready = False
                # If the task was ready, we're done
                if ready:
                    ready_tasks.append(task)
            
            ready_futures = []
            # If we found a ready task, we run it
            if len(ready_tasks) != 0:
                for task in ready_tasks:
                    if len(ready_list) == 0:
                        x = client.submit(task.run)
                        ready_futures.append(x)
                        all_futures.append(x)
                    else:
                        x = client.submit(task.run, ready_list[len(ready_list)-1])
                        ready_futures.append(x)
                        all_futures.append(x)
                    
                    # Mark its output files as produced
                    for f in task.outputfiles:
                        all_output_files[f] = True
                    
                    self.tasks.remove(task)
                ready_list.append(ready_futures)
            else:
                # This should never happen
                sys.stderr.write("FATAL ERROR: No ready task found\n")
                sys.exit(1)

        sys.stderr.write("Running workflow in parallel with DASK client...\n")
        start = time.time()
        client.gather(all_futures)
        end = time.time()
        sys.stderr.write("Workflow execution done in " +  str("{:.2f}".format(end - start)) + " seconds.\n")

'''
The functions below are written by scientists to generate
the structure of the workflow. The generate_workflow() function
returns a workflow object.
'''

def generate_workflow(center, degrees, bands):

    sys.stderr.write("Generating the Montage workflow...\n")

    wf = Workflow('montage')

    # region.hdr is the template for the ouput area
    generate_region_hdr(wf, center, degrees)

    band_id = 0
    color_band = {}
    for band_def in bands:
        band_id += 1
        (survey, band, color) = band_def.split(':')
        add_band(wf, band_id, center, degrees, survey, band, color)
        color_band[color] = band_id

    # if we have 3 bands in red, blue, green, try to create a color jpeg
    if 'red' in color_band and 'green' in color_band and 'blue' in color_band:
        color_png(wf, color_band['red'], color_band['green'], color_band['blue'])

    sys.stderr.write("Workflow generated.\n")
    return wf

def generate_region_hdr(wf, center, degrees):

    (crval1, crval2) = center.split()
    crval1 = float(crval1)
    crval2 = float(crval2)

    cdelt = 0.000277778
    naxis = int((float(degrees) / cdelt) + 0.5)
    crpix = (naxis + 1) / 2.0

    f = open('data/region.hdr', 'w')
    f.write('SIMPLE  = T\n')
    f.write('BITPIX  = -64\n')
    f.write('NAXIS   = 2\n')
    f.write('NAXIS1  = %d\n' %(naxis))
    f.write('NAXIS2  = %d\n' %(naxis))
    f.write('CTYPE1  = \'RA---TAN\'\n')
    f.write('CTYPE2  = \'DEC--TAN\'\n')
    f.write('CRVAL1  = %.6f\n' %(crval1))
    f.write('CRVAL2  = %.6f\n' %(crval2))
    f.write('CRPIX1  = %.6f\n' %(crpix))
    f.write('CRPIX2  = %.6f\n' %(crpix))
    f.write('CDELT1  = %.9f\n' %(-cdelt))
    f.write('CDELT2  = %.9f\n' %(cdelt))
    f.write('CROTA2  = %.6f\n' %(0.0))
    f.write('EQUINOX = %d\n' %(2000))
    f.write('END\n')

    f.close()

    # we also need an oversized region which will be used in the first part of the 
    # workflow to get the background correction correct
    f = open('data/region-oversized.hdr', 'w')

    f.write('SIMPLE  = T\n')
    f.write('BITPIX  = -64\n')
    f.write('NAXIS   = 2\n')
    f.write('NAXIS1  = %d\n' %(naxis + 3000))
    f.write('NAXIS2  = %d\n' %(naxis + 3000))
    f.write('CTYPE1  = \'RA---TAN\'\n')
    f.write('CTYPE2  = \'DEC--TAN\'\n')
    f.write('CRVAL1  = %.6f\n' %(crval1))
    f.write('CRVAL2  = %.6f\n' %(crval2))
    f.write('CRPIX1  = %.6f\n' %(crpix + 1500))
    f.write('CRPIX2  = %.6f\n' %(crpix + 1500))
    f.write('CDELT1  = %.9f\n' %(-cdelt))
    f.write('CDELT2  = %.9f\n' %(cdelt))
    f.write('CROTA2  = %.6f\n' %(0.0))
    f.write('EQUINOX = %d\n' %(2000))
    f.write('END\n')

    f.close()

def add_band(wf, band_id, center, degrees, survey, band, color):
    global verbose

    if verbose:
        redirect = None
    else:
        redirect = subprocess.DEVNULL

    band_id = str(band_id)

    sys.stderr.write('\tAdding band %s (%s %s -> %s)\n' %(band_id, survey, band, color))

    # data find - go a little bit outside the box - see mExec implentation
    degrees_datafind = str(float(degrees) * 1.42)
    cmd = 'mArchiveList %s %s \'%s\' %s %s data/%s-images.tbl' \
          %(survey, band, center, degrees_datafind, degrees_datafind, band_id)
    if (verbose):
        sys.stderr.write('\tRunning sub command: ' + cmd + "\n")
    if subprocess.call(cmd, shell=True, stderr=redirect, stdout=redirect) != 0:
        sys.stderr.write('\tCommand ' + cmd + ' failed!')
        sys.exit(1)

    # image tables
    raw_tbl = '%s-raw.tbl' %(band_id)
    projected_tbl = '%s-projected.tbl' %(band_id)
    corrected_tbl = '%s-corrected.tbl' %(band_id)
    cmd = 'cd data && mDAGTbls %s-images.tbl region-oversized.hdr %s %s %s' \
          %(band_id, raw_tbl, projected_tbl, corrected_tbl)
    if (verbose):
        sys.stderr.write('\tRunning sub command: ' + cmd + "\n")
    if subprocess.call(cmd, shell=True, stderr=redirect, stdout=redirect) != 0:
        sys.stderr('\tCommand' + cmd + '  failed!\n')
        sys.exit(1)
    
    # diff table
    cmd = 'cd data && mOverlaps %s-raw.tbl %s-diffs.tbl' \
          %(band_id, band_id)
    if (verbose):
        sys.stderr.write('\tRunning sub command: ' + cmd + '\n')
    if subprocess.call(cmd, shell=True, stderr=redirect, stdout=redirect) != 0:
        sys.stderr.write('\tCommand' + cmd + '  failed!\n')
        sys.exit(1)

    # statfile table
    t = ascii.read('data/%s-diffs.tbl' %(band_id))
    # make sure we have a wide enough column
    t['stat'] = '                                                                  '
    for row in t:
        base_name = re.sub('(diff\.|\.fits.*)', '', row['diff'])
        row['stat'] = '%s-fit.%s.txt' %(band_id, base_name)
    ascii.write(t, 'data/%s-stat.tbl' %(band_id), format='ipac')

    # for all the input images in this band, and them to the rc, and
    # add reproject tasks
    data = ascii.read('data/%s-images.tbl' %(band_id))  
    
    for row in data:
        
        base_name = re.sub('\.fits.*', '', row['file'])

        # add an entry to the replica catalog
        wf.add_file_to_download('ipac', base_name + '.fits', row['URL'])

        # projection task
        j = Task('mProject')
        in_fits = base_name + '.fits'
        projected_fits = 'p' + base_name + '.fits'
        area_fits = 'p' + base_name + '_area.fits'
        j.add_inputs('region-oversized.hdr', in_fits)
        j.add_outputs(projected_fits, area_fits, stage_out=False)
        j.add_args('-X', in_fits, '-z', '0.1', projected_fits, 'region-oversized.hdr')
        
        wf.add_tasks(j)

    fit_txts = []
    data = ascii.read('data/%s-diffs.tbl' %(band_id))
    for row in data:
        
        base_name = re.sub('(diff\.|\.fits.*)', '', row['diff'])

        # mDiffFit task
        j = Task('mDiffFit')
        plus = 'p' + row['plus']
        plus_area = re.sub('\.fits', '_area.fits', plus)
        minus = 'p' + row['minus']
        minus_area = re.sub('\.fits', '_area.fits', minus)
        fit_txt = '%s-fit.%s.txt' %(band_id, base_name)
        diff_fits = '%s-diff.%s.fits' %(band_id, base_name)
        j.add_inputs(plus, plus_area, minus, minus_area, 'region-oversized.hdr')
        j.add_outputs(fit_txt, stage_out=False)
        j.add_args('-d', '-s', fit_txt, plus, minus, diff_fits, 'region-oversized.hdr')
        wf.add_tasks(j)
        fit_txts.append(fit_txt)

    # mConcatFit
    j = Task('mConcatFit')
    stat_tbl = '%s-stat.tbl' %(band_id)
    j.add_inputs(stat_tbl)
    for fit_txt in fit_txts:
        j.add_inputs(fit_txt)
    fits_tbl = '%s-fits.tbl' %(band_id)
    j.add_outputs(fits_tbl, stage_out=False)
    j.add_args(stat_tbl, fits_tbl, '.')
    wf.add_tasks(j)

    # mBgModel
    j = Task('mBgModel')
    images_tbl = '%s-images.tbl' %(band_id)
    corrections_tbl = '%s-corrections.tbl' %(band_id)
    j.add_inputs(images_tbl, fits_tbl)
    j.add_outputs(corrections_tbl, stage_out=False)
    j.add_args('-i', '100000', images_tbl, fits_tbl, corrections_tbl)
    wf.add_tasks(j)

    # mBackground
    data = ascii.read('data/%s-raw.tbl' %(band_id))  
    for row in data:
        base_name = re.sub('(diff\.|\.fits.*)', '', row['file'])

        # mBackground task
        j = Task('mBackground')
        projected_fits = 'p' + base_name + '.fits'
        projected_area = 'p' + base_name + '_area.fits'
        corrected_fits = 'c' + base_name + '.fits'
        corrected_area = 'c' + base_name + '_area.fits'
        j.add_inputs(projected_fits, projected_area, projected_tbl, corrections_tbl)
        j.add_outputs(corrected_fits, corrected_area, stage_out=False)
        j.add_args('-t', projected_fits, corrected_fits, projected_tbl, corrections_tbl)
        wf.add_tasks(j)

    # mImgtbl - we need an updated corrected images table because the pixel offsets and sizes need
    # to be exactly right and the original is only an approximation
    j = Task('mImgtbl')
    updated_corrected_tbl = '%s-updated-corrected.tbl' %(band_id)
    j.add_inputs(corrected_tbl)
    j.add_outputs(updated_corrected_tbl, stage_out=False)
    j.add_args('.', '-t', corrected_tbl, updated_corrected_tbl)
    data = ascii.read('data/%s-corrected.tbl' %(band_id))  
    for row in data:
        base_name = re.sub('(diff\.|\.fits.*)', '', row['file'])
        projected_fits = base_name + '.fits'
        j.add_inputs(projected_fits)
    wf.add_tasks(j)

    # mAdd
    j = Task('mAdd')
    mosaic_fits = '%s-mosaic.fits' %(band_id)
    mosaic_area = '%s-mosaic_area.fits' %(band_id)
    j.add_inputs(updated_corrected_tbl, 'region.hdr')
    j.add_outputs(mosaic_fits, mosaic_area, stage_out=True)
    j.add_args('-e', updated_corrected_tbl, 'region.hdr', mosaic_fits)
    data = ascii.read('data/%s-corrected.tbl' %(band_id))  
    for row in data:
        base_name = re.sub('(diff\.|\.fits.*)', '', row['file'])
        corrected_fits = base_name + '.fits'
        corrected_area = base_name + '_area.fits'
        j.add_inputs(corrected_fits, corrected_area)
    wf.add_tasks(j)

    # mViewer - Make the JPEG for this channel
    j = Task('mViewer')
    mosaic_png = '%s-mosaic.png' %(band_id)
    j.add_inputs(mosaic_fits)
    j.add_outputs(mosaic_png, stage_out=True)
    j.add_args('-ct', '1', '-gray', mosaic_fits, '-1s', 'max', 'gaussian', \
               '-png', mosaic_png)
    wf.add_tasks(j)

def color_png(wf, red_id, green_id, blue_id):

    red_id = str(red_id)
    green_id = str(green_id)
    blue_id = str(blue_id)

    # mJPEG - Make the JPEG for this channel
    j = Task('mViewer')
    mosaic_png = 'mosaic-color.png'
    red_fits = '%s-mosaic.fits' %(red_id)
    green_fits = '%s-mosaic.fits' %(green_id)
    blue_fits = '%s-mosaic.fits' %(blue_id)
    j.add_inputs(red_fits, green_fits, blue_fits)
    j.add_outputs(mosaic_png, stage_out=True)
    j.add_args( \
            '-red', red_fits, '-0.5s', 'max', 'gaussian-log', \
            '-green', green_fits, '-0.5s', 'max', 'gaussian-log', \
            '-blue', blue_fits, '-0.5s', 'max', 'gaussian-log', \
            '-png', mosaic_png)
    wf.add_tasks(j)




'''
Main 
'''

if __name__ == '__main__':

    # Parse command-line arguments
    parser = argparse.ArgumentParser()

    parser.add_argument('--work-dir', action = 'store', dest = 'work_dir',
                        help = 'Work directory to chdir to')
    parser.add_argument('--verbose', action = 'store_true', dest = 'verbose',
                        help = 'Increase level of debug output')
    parser.add_argument('--center', action = 'store', dest = 'center',
                        help = 'Center of the output, for example \"56.5 23.75\"')
    parser.add_argument('--degrees', action = 'store', dest = 'degrees',
                        help = 'Number of degrees of side of the output')
    parser.add_argument('--band', action = 'append', dest = 'bands',
                        help = 'Band definition. Example: dss:DSS2B:red')
    args = parser.parse_args()
    
    verbose = args.verbose

    if args.center == None:
        sys.stderr.write("--center argument required\n")
        sys.exit(1)

    if args.degrees == None:
        sys.stderr.write("--degrees argument required\n")
        sys.exit(1)

    if args.bands == None:
        sys.stderr.write("--band argument required\n")
        sys.exit(1)

    # Changing working directory if --work-dir argument it passed
    if args.work_dir:
        os.chdir(args.work_dir)

    # Create the ./data directory if it does not exist
    if not os.path.isdir("./data"):
        sys.stderr.write("data/ directory does not exist, creating it...\n")
        os.mkdir("data")

    # Clean up data directory of the .tbl and .hdr files, if any
    os.system("rm -f ./data/*.tbl ./data/*.hdr")

    # creating DASK client
    client = Client()

    # Generate the workflow object
    wf = generate_workflow(args.center, args.degrees, args.bands)

    # Download all input FITS files, if not already present
    wf.download_all_input_files()

    # Run the workflow sequentially
    wf.run()

