# How to Run the Faust Application

## 1. Navigate into the `app` Directory
Change the current working directory to where your Faust application files are located:

```bash
cd ./app
```
## 2. Install all the Python packages listed in the requirements.txt file:

```bash
pip install -r ./requirements.txt
```

## 3. Run the Faust application as a worker process:
```bash
faust -A stream_proccessing worker -l info
```