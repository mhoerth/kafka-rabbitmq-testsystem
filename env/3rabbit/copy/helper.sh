# cluster rabbit2 with rabbit1
docker exec -it copy_rabbit2_1 rabbitmqctl stop_app
docker exec -it copy_rabbit2_1 rabbitmqctl reset
docker exec -it copy_rabbit2_1 rabbitmqctl join_cluster rabbit@rabbit1
docker exec -it copy_rabbit2_1 rabbitmqctl start_app
# cluster rabbit3 with rabbit1
docker exec -it copy_rabbit3_1 rabbitmqctl stop_app
docker exec -it copy_rabbit3_1 rabbitmqctl reset
docker exec -it copy_rabbit3_1 rabbitmqctl join_cluster rabbit@rabbit1
docker exec -it copy_rabbit3_1 rabbitmqctl start_app