# Databricks Telecom Bundle

Databricks Telecom Bundle is a Databricks Asset Bundle (DABS) that deploys all the necessary assets to showcase a Data Warehouse workload with synthetic generated telecom billing related data.

This bundle provides a complete end-to-end demonstration of a modern data platform, including:
- **Synthetic data generation** for telecom billing scenarios
- **ETL pipelines** for data ingestion and transformation (Bronze, Silver, Gold layers)
- **Data warehouse tables** with proper relationships for joins and aggregations
- **Interactive dashboards** for data visualization
- **Serverless workflow orchestration** for automated processing

The tables have well-defined relationships, making it easy to perform joins, aggregations, and leverage advanced features like Genie Spaces for natural language querying.

## Deployment Options

### Option 1: Deploy from Databricks Workspace

1. Clone this repository directly in your Databricks workspace
2. Follow the workspace tutorial: [Databricks Asset Bundle Workspace Tutorial](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-tutorial)
3. Deploy the bundle using the workspace interface

### Option 2: Deploy using Databricks CLI

1. Install the Databricks CLI:
   ```bash
   pip install databricks-cli
   ```

2. Authenticate to your Databricks workspace:
   ```bash
   databricks configure
   ```

3. Clone this repository locally:
   ```bash
   git clone <repository-url>
   cd databricks_telecom_bundle
   ```

4. Deploy the bundle:
   ```bash
   # For development environment
   databricks bundle deploy --target dev
   
   # For production environment
   databricks bundle deploy --target prod
   ```

## Configuration

### Variable Overrides

You can customize the deployment by overriding variables defined in `databricks.yml`. The main variable to focus on is:

- `catalog_name`: The catalog where all schemas and tables will be created (default: `databricks_telecom_bundle`)

Other configurable variables include schema names for different layers:
- `schema_ingestion`: Raw data ingestion schema
- `schema_customer_bronze/silver/gold`: Customer data schemas by layer
- `schema_billing_bronze/silver/gold`: Billing data schemas by layer
- `schema_resource_bronze/silver`: Resource data schemas by layer

#### Override Methods

You can override variables using several methods:

1. **Command-line variables**:
   ```bash
   databricks bundle deploy --target prod --var catalog_name=my_custom_catalog
   ```

2. **Environment variables**:
   ```bash
   export BUNDLE_VAR_catalog_name=my_custom_catalog
   databricks bundle deploy --target prod
   ```

3. **Variable overrides file**: Create a `.databricks/bundle/<target>/variable-overrides.json` file:
   ```json
   {
     "catalog_name": "my_custom_catalog",
     "schema_ingestion": "my_ingestion_schema",
     "schema_customer_gold": "my_customer_gold_schema"
   }
   ```

For more details on variable override methods and precedence, see the [Databricks Asset Bundle variables documentation](https://docs.databricks.com/aws/en/dev-tools/bundles/variables#custom-variables).

## Running the Bundle

### Deployment Time
⏱️ **Important**: The bundle runs on serverless workflows and takes approximately **30 minutes** to complete the full deployment and data generation process.

### Starting the Workflow

1. After cloning and deploying the bundle, navigate to your Databricks workspace
2. Go to **Workflows** section
3. Start the workflow named **`databricks_telecom_bundle`**
4. Monitor the progress through the workflow UI

### Prerequisites
- The catalog specified in your configuration should already exist in your workspace
- Ensure you have appropriate permissions to create schemas, tables, and workflows

## Dashboard Access

📊 **Dashboard Limitation**: The dashboard will only work if you deploy in **production mode** (`--target prod`). This is because Databricks Asset Bundles don't yet support variable usage in dashboards, so the dashboard is configured with production-specific references.

To access the dashboard:
1. Deploy using `--target prod`
2. Complete the workflow execution
3. Navigate to **Dashboards** in your workspace
4. Find the dashboard named according to your catalog configuration

## Project Structure

The bundle is organized into several main components:

- **`src/geracao_dados/`**: Synthetic data generation logic
- **`src/batch_ingestion/`**: Data ingestion pipelines (Bronze and Silver layers)
- **`src/data_warehousing/`**: Data warehouse implementation (Gold layer, dimensions, facts)
- **`src/bundle_orchestrator/`**: Workflow orchestration configuration

## Features Demonstrated

- **Medallion Architecture**: Bronze, Silver, and Gold data layers
- **Delta Live Tables**: Real-time data processing pipelines
- **Serverless Compute**: Cost-effective, auto-scaling compute resources
- **Data Relationships**: Properly modeled telecom billing data with foreign keys
- **Advanced Analytics**: Ready for complex queries and aggregations
- **Genie Spaces**: Natural language querying capabilities
- **Interactive Dashboards**: Business intelligence visualization

## Support

For more information about Databricks Asset Bundles, see the [official documentation](https://docs.databricks.com/dev-tools/bundles/index.html).
