To install and test without DDA  (INTEGRAL backend) access

```bash
git clone https://github.com/oda-hub/dispatcher-plugin-integral
git clone https://github.com/oda-hub/dispatcher-app
 
cd dispatcher-app/
export PYTHONPATH=$HOME/work/oda/test-install/dispatcher-plugin-integral

export DISPATCHER_ENFORCE_TOKEN=yes # this is compatible with current test set and policy, to discuss https://github.com/oda-hub/dispatcher-app/issues/12
python -m pytest tests/ -sv --maxfail=1 -m 'not dda'
```
