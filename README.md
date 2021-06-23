This file gives a summary of all the individual code files and how to execute them.

input.csv - This is the dataset file from which the python script gets the input. This file is not included in the submission due to its massive size but it can be downloaded here. https://csr.lanl.gov/data-fence/1622617257/-0SFtafWP_yNOBqLkD0m8LmmTbs=/unified-host-network-dataset-2017/netflow/netflow_day-02.bz2

GUI code:

proj.py - This is the main file which needs to be executed as follows: 
streamlit run proj.py
This will open a new tab in your default browser. After executing the code you should monitor the terminal and once the message "Ready to Recieve" appears input.py file should be executed.

input.py - Python script that reads csv file containing dataset. It can be executed as follows:
python input.py

Load shedding techniques:
Note: All these codes should be executed along with input.py code similar to proj.py code.

proj_uniform.py - Pyspark script with uniform sampling implemented. It can be executed as follows:
spark-submit proj_uniform.py
Note: This script does not have a GUI to display instead it outputs the results on the terminal

proj_random.py -  Pyspark script with random sampling implemented. It can be executed as follows:
spark-submit proj_random.py
Note: This script does not have a GUI to display instead it outputs the results on the terminal

Static result:

static.py - Pyspark script to get results of static data. It can be executed as follows:
spark-submit static.py

