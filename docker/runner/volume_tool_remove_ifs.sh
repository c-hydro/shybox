for VOL in case_study_destine_runner_ifs_geo \
		   case_study_destine_runner_ifs_restart \
           case_study_destine_runner_ifs_in \
           case_study_destine_runner_ifs_out \
           case_study_destine_runner_ifs_log \
           case_study_destine_runner_ifs_info \
           case_study_destine_runner_ifs_tmp
do
  echo "-----------------------------------"
  echo "Cleaning volume: $VOL"

  # Remove containers using volume
  docker rm -f $(docker ps -aq --filter volume=$VOL) 2>/dev/null || true

  # Remove volume
  docker volume rm $VOL 2>/dev/null || true
done

