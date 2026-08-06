for VOL in case_study_destine_runner_icon_geo \
		   case_study_destine_runner_icon_restart \	
		   case_study_destine_runner_icon_in \
           case_study_destine_runner_icon_out \
           case_study_destine_runner_icon_log \
           case_study_destine_runner_icon_info \
           case_study_destine_runner_icon_tmp
do
  echo "-----------------------------------"
  echo "Cleaning volume: $VOL"

  # Remove containers using volume
  docker rm -f $(docker ps -aq --filter volume=$VOL) 2>/dev/null || true

  # Remove volume
  docker volume rm $VOL 2>/dev/null || true
done

