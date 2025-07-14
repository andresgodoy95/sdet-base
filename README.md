### **Assignment Story: Duff Beer Inc.**

**Company Background:**
Duff Beer Inc. is a well-known beverage company that specializes in selling different beer products. They have a growing network of point-of-sale channels including direct sales to retailers, online orders, and B2B partnerships. As their sales increase, Duff Beer Inc. needs a robust system to track and analyze their order data in real-time. They are currently facing difficulties in processing their large volumes of order data and want to automate this using AWS services.

Your task is to help Duff Beer Inc. by building an ETL pipeline to process their orders, summarize them, and expose a report through an API.

---

### **Sample CSV File: Orders Data**

Here is a sample structure of the CSV file that Duff Beer Inc. will upload to S3:

| client_id | client_name | order_id | product_id | product_description | product_price | product_ccf | product_volume | point_of_sale_channel | status    |
|-----------|-------------|----------|------------|----------------------|---------------|----------------|-------------|-----------------------|-----------|
| 1001      | Moe's Tavern| 5678     | 102        | Duff Classic          | 2.50          | 6              | 0.33        | Retail                | created   |
| 1002      | Kwik-E-Mart | 5679     | 103        | Duff Lite             | 2.00          | 12             | 0.5         | B2B                   | delivered |
| 1003      | Krusty Burger| 5680     | 104        | Duff Dry              | 3.00          | 8              | 0.25        | Online                | broken    |
| 1004      | Springfield Mall| 5681  | 105        | Duff Special          | 4.00          | 24             | 1.00        | Retail                | created   |
| 1005      | Moe's Tavern| 5682     | 102        | Duff Classic          | 2.50          | 6              | 0.33        | B2B                   | delivered |

- **client_id**: The ID of the client placing the order.
- **client_name**: The name of the client.
- **order_id**: Unique ID for the order.
- **product_id**: Unique ID for the product.
- **product_description**: Name or description of the product ordered.
- **product_price**: Price of the product per unit.
- **product_ccf**: Number of units ordered.
- **product_volume**: Volume per unit (in liters or specific measure).
- **point_of_sale_channel**: Channel through which the order was placed (e.g., Retail, Online, B2B).
- **status**: The current status of the order. Possible values are:
  - `created`: Order has been created.
  - `delivered`: Order has been successfully delivered.
  - `broken`: The order was damaged or had issues during delivery.

---

### **ETL Pipeline Challenge**:
Your task is to set up an automated ETL pipeline to process Duff Beer Inc.'s order data. The pipeline should be fully serverless, utilizing AWS services to extract, transform, and load the data. Below are the steps and expectations:

Data Ingestion:

The pipeline begins when a CSV file containing order data is uploaded to an S3 bucket. This file serves as the input to your ETL process.

Event Trigger:

Set up an event notification that triggers an ETL process as soon as the CSV file is uploaded. This event should invoke AWS services to process the data.

ETL Process:

The ETL process can be designed using:

**AWS Step Functions**, which can orchestrate multiple tasks in a sequential or parallel flow.
**Lambda Functions** for data processing and transformation (e.g., reading the CSV, aggregating data, and performing calculations).
**AWS Glue Jobs**, if you opt for a more scalable ETL tool (Glue is optional but encouraged if appropriate for your solution).

The ETL process should:

1.  Read the data from the CSV file.
      Perform necessary transformations, such as aggregating order information (e.g., calculating total sales per client).
      Save the transformed data in a separate S3 bucket for later querying.
      Data Storage & Querying:

2. After the ETL process, the transformed data should be saved in a queryable format in an S3 bucket.

3. Set up AWS Athena to query this processed data, enabling the retrieval of meaningful insights such as:
    Total orders per client
    Total sales per product
    Status of orders (e.g., delivered, broken, created)
    Expose the Data via an API:

4. Create a REST API using API Gateway that allows users to query the processed data.
    The API should expose endpoints that can return specific reports based on the processed data, such as:
    Orders for a specific client
    Sales breakdown by product
    Order status reports
    Requirements & Best Practices:

**Aditional Info**

- The entire solution should follow a serverless architecture, ensuring scalability and minimal operational overhead.
- Implement proper error handling and logging to ensure robustness, especially in the Lambda functions and API Gateway.
- Write clean and modular code that is easy to understand and maintain.

**Bonus Points:**
- Implement additional data validation, such as checking for malformed CSV rows or missing data.
- Optimize the pipeline to handle large datasets efficiently (e.g., by using batch processing or partitioning in Athena).
- Provide cost-optimization suggestions, ensuring minimal resource usage for maximum efficiency.

### **Additional Task: Data Modeling:**

Now that you've built the ETL pipeline for processing orders, Duff Beer Inc. also wants to improve their data structure for analytics. Based on the processed dataset, create as many models as you can. These models should help the company analyze their data efficiently.

You can include, but are not limited to, the following models:

- Clients Model: A model that tracks information about Duff Beer Inc.'s clients (e.g., client_id, client_name, point_of_sale_channel, etc.).
- Products Model: A model that organizes data about the products Duff Beer Inc. sells (e.g., product_id, product_description, product_price, product_volume, etc.).
- Orders Model: A model that keeps records of orders (e.g., order_id, client_id, product_id, status, etc.).

Feel free to extend or create additional models that you think would benefit Duff Beer Inc. in analyzing their sales and order data.

