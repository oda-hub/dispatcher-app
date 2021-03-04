To install and test without DDA  (INTEGRAL backend) access

```bash
git clone https://github.com/oda-hub/dispatcher-plugin-integral
git clone https://github.com/oda-hub/dispatcher-app
 
cd dispatcher-app/
export PYTHONPATH=$HOME/work/oda/test-install/dispatcher-plugin-integral
python -m pytest tests/ -sv --maxfail=1 -m 'not dda'
```
