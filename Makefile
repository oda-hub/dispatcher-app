test:
	python -m pytest tests/test_server_basic.py::test_isgri_image_fixed_done -sv --full-trace  --maxfail=1 --log-cli-level=DEBUG

clean:
	rm -rfv scratch_sid_* data_* tmp_Exception_*
