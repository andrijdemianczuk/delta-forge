from databricks.connect import DatabricksSession
from pyspark.sql.types import *
from format.DeltaDataframeHelper import DeltaDataframeHelper

spark = DatabricksSession.builder.profile("ad-dbx-small").getOrCreate()

if __name__ == "__main__":

    data = [("John Roland", "35,0", "M", "180.0"), ("Mary Percy", "30,0", "F", "158.0"),
            ("Mike Howe", "35,0", "X", "172.0")]

    schema = StructType([
        StructField("Full Name", StringType()),
        StructField("Age", StringType()),
        StructField("Identified Gender", StringType()),
        StructField("Height", StringType()),
    ])

    df = spark.createDataFrame(data, schema)

    df.printSchema()

    fixed_data: list = []

    for col in df.columns:
        fixed_data.append(f"{str(col).strip()}")

    dfh = DeltaDataframeHelper()

    #Using the global search string replace for columns in fixed_data[]
    df1 = dfh.substringReplaceData(col_names=fixed_data)(df=df, findChars=",", replaceChars=".")
    df1.show()

    #Using the column fixer to set all cols to lowercase with stripped out whitespaces
    fixed_cols = dfh.formatDataframeCols(df=df1)
    df2 = df1.selectExpr(fixed_cols)
    df2.show()

    #Cast a group of columns
    cols_to_cast = ['age', 'height']
    df3 = dfh.castColTypes(cols_to_cast)(df=df2, targetType=DoubleType())
    df3.printSchema()
