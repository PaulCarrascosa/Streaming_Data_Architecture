"""PySpark job that reads image files (binary) and runs Tesseract OCR.
 
Usage (local testing):
 
  spark-submit pyspark_ocr.py --input "/tmp/bytewax_images/*" --output /tmp/ocr_results
 
Notes:
- The machine(s) running this job must have the `tesseract` binary installed and
   accessible on PATH (e.g. `sudo apt-get install -y tesseract-ocr`).
- Also install Python packages: pillow, pytesseract.
"""
 
import base64
import io
import os
 
from PIL import Image
import numpy
import pytesseract
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
 
from pyspark.sql.types import StringType
 
 
def tesseract_with_spark(list_images: list[str]):
    print(f"tesseract_with_spark : {str(list_images)}")
    def ocr_image_bytes(raw: str) -> str:
        img_bytes = base64.b64decode(raw)
        img = Image.open(io.BytesIO(img_bytes))
        if img.mode != "RGB":
            img = img.convert("RGB")
        return pytesseract.image_to_string(img)
    driver_host = os.getenv("DRIVER_HOST", "host.docker.internal")
    spark_master_host = os.getenv("SPARK_MASTER_HOST", "localhost")
    spark = (SparkSession.builder.appName("pyspark_tesseract_ocr")
        .master(f"spark://{spark_master_host}:7077")
        .config("spark.driver.host", driver_host)
        .config("spark.driver.port", "5001")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.blockManager.port", "5002")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("DEBUG")
    np_array = numpy.array(list_images)
    df = spark.createDataFrame(np_array)
    ocr_udf = udf(ocr_image_bytes, StringType())
    result_df = df.withColumn(
        "extracted_text",
        ocr_udf(col("value"))
    )
    results = result_df.select("extracted_text").collect()
    results_list = [row.asDict() for row in results]
    spark.stop()
    return results_list
 