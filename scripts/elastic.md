

DELETE /cohort_all

------------------------


DELETE /cohort_indexes_info/_doc/foods
------------------------

PUT cohort_all/_settings
{
  "index.mapping.total_fields.limit": 2000
}
------------------------

PUT /new-shoot
{
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "properties": {
    "location": { "type": "geo_point" }
    }
  }
}

------------------------

PUT cohort_all/_mapping 
{
  "properties": {
    "general.point": { "type": "geo_point" }
  }
}

------------------------

PUT cohort_general_data/_mapping 
{
  "properties": {
    "point": { "type": "geo_point" }
  }
}

------------------------
# Click the Variables button, above, to create your own variables.
#GET ${exampleVariable1} // _search
# {
#  "query": {
#    "${exampleVariable2}": {} // match_all
#  }
#}