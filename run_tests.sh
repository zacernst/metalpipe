nohup ./start_json_server.sh &
PYTHONPATH=. pytest -v ./tests
kill -9 `ps aux | grep "json-server" | grep -v "grep"| awk '{print $2}'`
