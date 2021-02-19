test:
	python -m pytest tests/test_server_basic.py::test_isgri_image -sv --full-trace  --maxfail=1 --log-cli-level=DEBUG
