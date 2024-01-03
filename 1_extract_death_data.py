

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



