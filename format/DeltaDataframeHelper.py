# The singleton decorator keeps track of instances in a dictionary object. We can refer to this at any point of the execution.
# The getInstance function checks if an instance of the class exists. If not, it creates one; otherwise, it returns the existing instance.
# Applying the @singleton decorator to the class ensures it follows the singleton pattern. This acts as a wrapper function to ensure compliance.

from pyspark.sql import DataFrame, functions
from pyspark.sql.types import *
from pyspark.sql.functions import col


# Send in the current instance of the class to check or add it to the dictionary
def singleton(cls):
    instances = {}

    def getInstance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return getInstance


# Apply the singleton wrapper function to the instance of the class
@singleton
class DeltaDataframeHelper:
    """
    A class to format data for Delta Lake storage using PySpark. Assists users in formatting PySpark dataframes to be Delta format compliant

    ...

    Attributes
    ----------
    isInstance : bool (optional)
        A supporting flag to check if the class has been instantiated or not. Forces override of singleton instance.

    Methods
    -------
    formatDataframeCols(self, df: DataFrame) -> list
        returns: A formatted python list of column names to be used in the Delta Lake schema
        Replaces all white spaces (" ") with underscores ("_") to support Delta Lake column naming conventions.

    substringReplaceData(self, col_names: list = ["none"])(df: DataFrame, findChars: str = ",", replaceChars: str = ".")
        returns: A sub-function to be used to format numeric columns to be compatible with Delta Lake.
        Finds and replaces all instances of a substring with user-specified strings in the column names passed in. Can be used for more than numeric data

    castColTypes(self, col_names: list = ["none"])(df: DataFrame, targetType = tringType())
        returns: A sub-function to be used to cast columns to a specified type. Requires target type to be a pyspark.sql.types type.
        Casts all columns in the column names passed in to the target type.
    """

    def __init__(self, isInstance: bool = True):
        """
        Parameters
        ----------
        isInstance : bool (optional)
            A supporting flag to check if the class has been instantiated or not. Forces override of singleton instance.
        """

        self.isInstance = isInstance

    def formatDataframeCols(self, df: DataFrame) -> list:
        """
        Removes all whitespaces from all columns and replaces them with underscores ("_") to support Delta Lake column naming conventions.

        Parameters
        ----------
        df : DataFrame
            The PySpark dataframe to be formatted

        Returns
        -------
        list : list
            A python list of column names to be used in the Delta Lake schema

        Example
        -------
        # Format all columns in a dataframe (df) to Delta Lake column naming conventions
        import delta-forge.format
        dfh = format.DeltaFormatHelper.DeltaFormatHelper()
        df = dfh.formatDataframeCols(df=df)

        Raises
        ------
        GeneralError
        """

        try:
            fixed_col_list: list = []
            for col in df.columns:
                fixed_col_list.append(
                    f"`{str(col).strip()}` as {str(col).strip().replace(' ','_').lower()}"
                )
            return fixed_col_list
        except Exception as e:
            raise e

    def substringReplaceData(self, col_names: list = ["none"]):
        """
        Replaces all instances of a substring with user-specified strings in the column names passed in.

        Parameters
        ----------
        col_names : list (optional)
            A python list of column names to be formatted. Defaults to ["none"].

        Inner Parameters
        ----------
        df : DataFrame
            The PySpark dataframe to be formatted

        findChars : str (optional)             
            The substring to be replaced. Defaults to ",".        
            
        replaceChars : str (optional)             
            The string to replace the substring with. Defaults to ".".

        Example
        -------
        # Replace all instances of a comma with a period in column labeled col1 and col2 in a dataframe (df)
        import delta-forge.format
        dfh = format.DeltaFormatHelper.DeltaFormatHelper()
        df = dfh.substringReplaceData(["col1","col2"])(df, ",", ".")

        Raises
        ------
        GeneralError
        """

        def inner(df: DataFrame, findChars: str = ",", replaceChars: str = "."):
            try:
                for col_name in col_names:
                    df = df.withColumn(
                        col_name,
                        functions.regexp_replace(
                            f"{col_name}", r"[%s]" % findChars, replaceChars
                        ),
                    )
                return df
            except Exception as e:
                raise e

        return inner

    def castColTypes(self, col_names: list = ["none"]):
        """
        Casts all columns in the column names passed in to the target type.

        Parameters
        ----------
        col_names : list (optional)
            A python list of column names to be formatted. Defaults to ["none"].

        Inner Parameters
        ----------
        df : DataFrame
            The PySpark dataframe to be formatted

        targetType : pyspark.sql.types type (optional)
            The target type to cast all columns to (PySpark Format). Defaults to StringType()

        Example
        -------
        # Cast columns labeled col1 and col2 in a dataframe (df) to double type
        import delta-forge.format
        dfh = format.DeltaFormatHelper.DeltaFormatHelper()
        df = dfh.castColTypes(["col1","col2"])(df, DoubleType())

        Raises
        ------
        GeneralError
        """

        def inner(df: DataFrame, targetType=StringType()):
            try:
                for col_name in col_names:
                    df = df.withColumn(col_name, col(col_name).cast(targetType))
                return df
            except Exception as e:
                raise e

        return inner