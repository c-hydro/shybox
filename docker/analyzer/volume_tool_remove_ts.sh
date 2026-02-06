for VOL in case_study_destine_analyzer_ts_geo \
           case_study_destine_analyzer_ts_in \
           case_study_destine_analyzer_ts_out \
           case_study_destine_analyzer_ts_log \
           case_study_destine_analyzer_ts_tmp
do
  echo "-----------------------------------"
  echo "Cleaning volume: $VOL"

  # Remove containers using volume
  docker rm -f $(docker ps -aq --filter volume=$VOL) 2>/dev/null || true

  # Remove volume
  docker volume rm $VOL 2>/dev/null || true
done

