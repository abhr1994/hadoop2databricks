## Welcome to GitHub Pages

You can use the [editor on GitHub](https://github.com/abhr1994/hadoop2databricks/edit/master/README.md) to maintain and preview the content for your website in Markdown files.

Whenever you commit to this repository, GitHub Pages will run [Jekyll](https://jekyllrb.com/) to rebuild the pages in your site, from the content in your Markdown files.

### Markdown

Markdown is a lightweight and easy-to-use syntax for styling your writing. It includes conventions for

```markdown
Syntax highlighted code block

# Header 1
## Header 2
### Header 3

- Bulleted
- List

1. Numbered
2. List

**Bold** and _Italic_ and `Code` text

[Link](url) and ![Image](src)
```

```markdown
# Create RDBMS Source

source /opt/infoworks/bin/env.sh;python /home/infoworks/abhi/create_rdbms_source.py --source_connection_file_path /home/infoworks/abhi/source_details.csv --source_creation_template /home/infoworks/abhi/templates/create_source_template.json --host_name localhost --host_port 2999 --auth_token ytWtzxFzXlmmA90eDTXkWnrnVPq3IZA3jRLIWxwnaQ8IVqDhcScLXDdHDDhgXygfu5earU0xLKvrLOVxdzNMkA== --cluster_template default_template

# Add source to cluster template

python add_source_to_clustertemplate.py --source_name Teradata_test --cluster_template default_template

# Script to configure the sources

source /opt/infoworks/bin/env.sh;python /home/infoworks/abhi/source_migration_v2.py --configuration_json_path /home/infoworks/abhi/source_AR_Test_TD.json --source_name AR_Test_TD --source_type rdbms --host_name localhost --host_port 2999 --auth_token ytWtzxFzXlmmA90eDTXkWnrnVPq3IZA3jRLIWxwnaQ8IVqDhcScLXDdHDDhgXygfu5earU0xLKvrLOVxdzNMkA== --cluster_template default_template

# Script to run the historical data migration

source /opt/infoworks/bin/env.sh;python /home/infoworks/abhi/run_migration.py --host adb-5996418727748488.8.azuredatabricks.net --token dapi6c880eecf5f80d33e31b4aa86d653a7c --cluster_id 0718-041317-gilt53 --param_file /home/infoworks/abhi/param_file.csv
```
