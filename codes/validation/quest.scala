REFERENCE CODE :
================

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType, StructType, StructField}

object MultiFormatFileValidation {

// Initialize Spark session
val spark: SparkSession = SparkSession.builder
.appName(&quot;MultiFormatFileValidation&quot;)
.master(&quot;local[*]&quot;) // Change this according to your cluster settings
.getOrCreate()

import spark.implicits._

// Function to load files dynamically based on file extension
def loadFileByExtension(filePath: String): DataFrame = {
val fileExtension = filePath.split(&quot;\\.&quot;).last.toLowerCase
fileExtension match {
case &quot;txt&quot; =&gt;
spark.read.option(&quot;header&quot;, &quot;false&quot;).option(&quot;inferschema&quot;, &quot;true&quot;).csv(filePath)
case &quot;parquet&quot; =&gt;
spark.read.parquet(filePath)
case &quot;avro&quot; =&gt;
spark.read.format(&quot;avro&quot;).load(filePath) // Requires spark-avro package
case _ =&gt;

throw new IllegalArgumentException(s&quot;Unsupported file format: .$fileExtension&quot;)
}
}

// Function to clean strings and remove any unwanted spaces or invisible characters
def cleanString(value: String): String = {
if (value != null) {
value.trim.replaceAll(&quot;[^\\x20-\\x7E]&quot;, &quot;&quot;) // Removes non-printable characters
} else {
&quot;&quot; // Replace null with empty string
}
}

// Function to validate header and trailer based on loaded DataFrame
def validateHeaderTrailer(df: DataFrame, fileType: String, metadataDF: DataFrame): Boolean = {
val (expectedVersion, expectedRecordCount) = getExpectedMetadata(fileType, metadataDF)

// Get header and trailer rows
val header = df.first() // Assuming the header is the first row
val trailer = df.collect().last // Assuming the trailer is the last row
val actualRecordCount = df.count() - 2 // Exclude header and trailer rows from the count

// Clean header and trailer values for comparison
val cleanedHeader = header.toSeq.take(3).map {
case value: String =&gt; cleanString(value)
case other =&gt; other.toString
}

val cleanedTrailer = trailer.toSeq.take(3).map {

case value: String =&gt; cleanString(value)
case other =&gt; other.toString
}

// Header and trailer validation conditions
val isHeaderValid = cleanedHeader(0) == fileType &amp;&amp;
cleanedHeader(1) == expectedVersion &amp;&amp;
cleanedHeader(2).toInt == expectedRecordCount

val isTrailerValid = cleanedTrailer(0) == &quot;TRAILER&quot; &amp;&amp;
cleanedTrailer(2).toInt == expectedRecordCount

// Validation result
isHeaderValid &amp;&amp; isTrailerValid &amp;&amp; actualRecordCount == expectedRecordCount
}

// Metadata function to fetch expected values from metadata DataFrame
def getExpectedMetadata(fileType: String, metadataDF: DataFrame): (String, Int) = {
val metadataRow = metadataDF.filter(col(&quot;file_type&quot;) === fileType).collect().headOption
metadataRow match {
case Some(row) =&gt;
val expectedVersion = row.getString(1)
val expectedRecordCount = row.getInt(2)
(expectedVersion, expectedRecordCount)
case None =&gt;
throw new Exception(s&quot;No metadata found for file type $fileType&quot;)
}
}

// Main function to handle multiple file types and validate header/trailer
def main(args: Array[String]): Unit = {
val filePath = &quot;C:/Users/Karthik Kondpak/Documents/data9.txt&quot; // Path to your file (adjust path)
val fileType = &quot;TYPE_A&quot; // Specify file type for metadata lookup

try {
// Load file dynamically based on extension
val df = loadFileByExtension(filePath)

// Define sample Metadata Table with expected values for each file type
val metadataSchema = StructType(List(
StructField(&quot;file_type&quot;, StringType, nullable = false),
StructField(&quot;expected_version&quot;, StringType, nullable = false),
StructField(&quot;expected_record_count&quot;, IntegerType, nullable = false)
))

val metadataData = Seq(
Row(&quot;TYPE_A&quot;, &quot;1&quot;, 20)
)

val metadataDF = spark.createDataFrame(
spark.sparkContext.parallelize(metadataData),
metadataSchema
)

// Validate header and trailer
val isValid = validateHeaderTrailer(df, fileType, metadataDF)
println(s&quot;Header and Trailer Validation Result for file type $fileType: $isValid&quot;)

// Proceed with processing only if validation is successful
if (isValid) {
// Sample transformation using when and otherwise
val processedDF = df.withColumn(&quot;status&quot;,
when($&quot;column1&quot;.isNull, &quot;Missing&quot;)
.otherwise(&quot;Available&quot;)
)

// Display the processed DataFrame
processedDF.show()
} else {
println(&quot;Validation failed. Data processing will not proceed.&quot;)
}
} catch {
case e: Exception =&gt; println(s&quot;Validation failed: ${e.getMessage}&quot;)
} finally {
spark.stop()
}
}
}

REFERENCE DATA
TYPE_A,1,22
1,karthik,200,hyd
2,mohangdsjhfedjkfdjkfjlfdljjkldjkfhjkfdbhjklfrdhjdfksbjkdfbdjksbgfdkljgjkfdsbgfdskjdsfj,300,hyd
3,jay,400,hyd
4,vijay,500,hyd
5,johar,600,hyd
6,ajay,800,hyd
7,A,700,hyd
8,b,800,hyd
9,c,900,hyd
10,d,1000,chen
11,e,1005,chen
12,f,1006,chen
13,g,1008,chen
14,h,1110,chen
15,i,1105,chen
16,j,1102,chen
17,hh,1089,chen
18,mm,1789,chen
19,bb,1000,chen
20,qq,2345,ap
TRAILER,1,22

10 CODING QUESTIONS

1. Calculate Average Salary by Department
Sample Data:
 Header: [&quot;EMPLOYEE&quot;, &quot;v1&quot;, &quot;6&quot;] (file type: EMPLOYEE, version: v1, expected record count: 6)
 Data:
Name | Department | Salary
---------|------------|-------
Karthik | IT | 70000
Pratik | IT | 75000
Hari | HR | 55000
Mohan | IT | 80000
Veer | HR | 60000
Veena | IT | 72000
 Trailer: [&quot;TRAILER&quot;, &quot;6&quot;] (indicating 6 records)
Processing: Calculate the average salary by department. Only proceed if the header indicates the
correct version and record count matches.

2. Count of Employees by Department and Gender
Sample Data:
 Header: [&quot;EMPLOYEE&quot;, &quot;v1&quot;, &quot;7&quot;]
 Data:
Name | Department | Gender
---------|------------|-------
Karthik | IT | Male
Pratik | IT | Male
Hari | HR | Male
Mohan | IT | Male
Veer | HR | Male
Veena | IT | Female
Mohana | HR | Female
 Trailer: [&quot;TRAILER&quot;, &quot;7&quot;]
Processing: Group by department and gender, and count employees. Ensure header and trailer
counts are validated before processing.

3. Calculate Total Sales by Region
Sample Data:
 Header: [&quot;SALES&quot;, &quot;v2&quot;, &quot;5&quot;]
 Data:
Region | SalesPerson | Sales
---------|-------------|------
North | Karthik | 10000
South | Pratik | 15000
East | Hari | 8000
West | Mohan | 12000
North | Veer | 7000
 Trailer: [&quot;TRAILER&quot;, &quot;5&quot;]
Processing: Sum sales by region. Proceed only if the total count and header metadata are correct.

4. Filter Out Employees in IT Department Earning Above 75K
Sample Data:
 Header: [&quot;EMPLOYEE&quot;, &quot;v1&quot;, &quot;5&quot;]
 Data:
Name | Department | Salary
---------|------------|-------
Karthik | IT | 70000
Pratik | IT | 75000
Mohan | IT | 80000
Veer | HR | 60000
Mohana | IT | 78000
 Trailer: [&quot;TRAILER&quot;, &quot;5&quot;]
Processing: Filter employees in the IT department earning more than 75,000. Only perform this
filtering if validation passes.

5. Identify Products with Zero Sales
Sample Data:
 Header: [&quot;PRODUCT_SALES&quot;, &quot;v1&quot;, &quot;5&quot;]
 Data:
Product | Sales
---------|------
Laptop | 10000
Mouse | 0
Keyboard | 5000
Monitor | 0
Printer | 7000
 Trailer: [&quot;TRAILER&quot;, &quot;5&quot;]
Processing: Filter products where sales are zero. Ensure validation of header and trailer before
filtering.

6. Calculate Age Group Distribution of Customers
Sample Data:
 Header: [&quot;CUSTOMER&quot;, &quot;v3&quot;, &quot;6&quot;]
 Data:
Name | Age
---------|---
Karthik | 30
Pratik | 45
Mohan | 20
Veer | 60
Veena | 25
Mohana | 33
 Trailer: [&quot;TRAILER&quot;, &quot;6&quot;]
Processing: Group customers into age ranges (e.g., 20–30, 31–40, etc.). Process only if header/trailer
validation passes.

7. Calculate Total Working Hours by Employee
Sample Data:
 Header: [&quot;TIMESHEET&quot;, &quot;v1&quot;, &quot;6&quot;]
 Data:
Name | Hours
---------|------
Karthik | 40
Pratik | 35
Mohan | 45
Veer | 30
Veena | 50
Mohana | 40
 Trailer: [&quot;TRAILER&quot;, &quot;6&quot;]
Processing: Sum total hours worked by each employee. Only process if validation is successful.

8. Identify Students Scoring Above 80%
Sample Data:
 Header: [&quot;STUDENTS&quot;, &quot;v1&quot;, &quot;5&quot;]
 Data:
Name | Score
---------|------
Karthik | 82
Pratik | 78
Mohan | 90
Veer | 85
Mohana | 76
 Trailer: [&quot;TRAILER&quot;, &quot;5&quot;]
Processing: Filter students with scores above 80. Ensure header and trailer validation before
filtering.

9. Calculate Maximum and Minimum Sales by Salesperson
Sample Data:
 Header: [&quot;SALES&quot;, &quot;v2&quot;, &quot;6&quot;]
 Data:
SalesPerson | Sales
------------|------
Karthik | 12000
Pratik | 15000
Mohan | 8000
Veer | 20000
Veena | 5000
Mohana | 18000
 Trailer: [&quot;TRAILER&quot;, &quot;6&quot;]
Processing: Calculate max and min sales for each salesperson. Only proceed if validation succeeds.

10. Identify Customers with Incomplete Addresses
Sample Data:
 Header: [&quot;CUSTOMER_INFO&quot;, &quot;v1&quot;, &quot;5&quot;]
 Data:
Name | Address
---------|--------
Karthik | &quot;123 Main St&quot;
Pratik | null
Mohan | &quot;456 Elm St&quot;
Veer | null
Veena | &quot;789 Oak St&quot;
 Trailer: [&quot;TRAILER&quot;, &quot;5&quot;]
Processing: Filter customers where address is missing (null). Execute only if header/trailer validation
is correct.

Code for Header and Trailer Validation with Processing
Here’s the code that performs header/trailer validation and proceeds with data processing only if
the validation is successful.
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions.
import org.apache.spark.sql.types.{StringType, IntegerType, StructType, StructField}

object HeaderTrailerValidation {

val spark: SparkSession = SparkSession.builder()
.appName(&quot;HeaderTrailerValidation&quot;)
.master(&quot;local[*]&quot;)
.getOrCreate()

import spark.implicits._

def validateHeaderTrailer(df: DataFrame, fileType: String, metadata: (String, Int)): Boolean = {
val header = df.first()
val trailer = df.collect().last
val actualCount = df.count() - 2 // Exclude header and trailer rows

val isHeaderValid = header.getString(0) == fileType &amp;&amp;
header.getString(1) == metadata._1 &amp;&amp;
header.getString(2).toInt == metadata._2

val isTrailerValid = trailer.getString(0) == &quot;TRAILER&quot; &amp;&amp;
trailer.getString(1).toInt == metadata._2

isHeaderValid &amp;&amp; isTrailerValid &amp;&amp; actualCount == metadata._2
}

def processData(df: DataFrame): Unit = {
val processedDF = df.filter($&quot;Department&quot; === &quot;IT&quot;).withColumn(&quot;status&quot;,
when($&quot;Salary&quot; &gt; 75000, &quot;High&quot;).otherwise(&quot;Low&quot;)

Seekho Bigdata Institute
)
processedDF.show()
}

def main(args: Array[String]): Unit = {
val sampleData = Seq(
Row(&quot;EMPLOYEE&quot;, &quot;v1&quot;, &quot;6&quot;),
Row(&quot;Karthik&quot;, &quot;IT&quot;, &quot;70000&quot;),
Row(&quot;Pratik&quot;, &quot;IT&quot;, &quot;75000&quot;),
Row(&quot;Hari&quot;, &quot;HR&quot;, &quot;55000&quot;),
Row(&quot;Mohan&quot;, &quot;IT&quot;, &quot;80000&quot;),
Row(&quot;Veer&quot;, &quot;HR&quot;, &quot;60000&quot;),
Row(&quot;TRAILER&quot;, &quot;6&quot;)
)
val schema = StructType(List(
StructField(&quot;Name&quot;, StringType, true),
StructField(&quot;Department&quot;, StringType, true),
StructField(&quot;Salary&quot;, IntegerType, true)
))

val df = spark.createDataFrame(spark.sparkContext.parallelize(sampleData), schema)

if (validateHeaderTrailer(df, &quot;EMPLOYEE&quot;, (&quot;v1&quot;, 6))) {
processData(df)
} else {
println(&quot;Header or Trailer validation failed!&quot;)
}
}
}
