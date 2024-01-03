"""
--Large Session--

Session designed for running Production pipelines on large administrative data, 
rather than just survey data. 
Will often develop using a sample and a smaller session then change to this 
once the pipeline is complete.

Details:
10g of memory and 5 executors
1g of memory overhead
5 cores, which is generally optimal on larger sessions
The default number of 200 partitions

Use case:
Production pipelines on administrative dataCannot be used in Dev Test, 
as it exceeds the 9 GB limit per executor

Example of actual usage:
One administrative dataset of 100 million rows
Many calculations"""

##############################################################################
# Import Modules 
##############################################################################

from pyspark.sql import SparkSession
import os
import sys
import IPython
from pyspark.sql import functions as F
import pandas as pd

pd.set_option("display.html.table_schema", True)

# Set your desired location
os.chdir(dir)



##############################################################################
# Import HALE Reusable Functions
##############################################################################

sys.path.insert(1, f'/home/cdsw/{os.getcwd().split("/")[3]}/hale_engineering/haleReusableFunctions')
import pysparkFunctions as pf




##############################################################################
# Get Spark UI
##############################################################################

url = "spark-%s.%s" % (os.environ["CDSW_ENGINE_ID"], os.environ["CDSW_DOMAIN"])
IPython.display.HTML("<a href=http://%s>Spark UI</a>" % url)


##############################################################################
# Setting Large Spark Session
##############################################################################

spark = pf.create_large_spark_session()


##############################################################################
# Run Script
##############################################################################


#pf.get_mortality_data?

death = pf.get_mortality_data(spark,"20110101","20221231", "deaths_20230116_std","occurrence")

#death.write.mode("overwrite").saveAsTable(fr'cen_dth_gps.deaths_2022_clean_T2')

pf.save_file_in_SQL(death, None, 
                    'deaths_2011_2022', 
                    suffix=True, project=project)



