cloud_function_name="$1"

result=''
# Checking if there is cloud function running
while [ "$result" != "Listed 0 items." ]
do
    first_date=$(date --date '-5 min' +"%Y-%m-%d %T")
    last_date=$(date +"%Y-%m-%d %T")
    result=((gcloud functions logs read --limit 1 --filter name=$cloud_function_name --start-time="$first_date" --end-time="$last_date") 2>&1)
    sleep 60
done