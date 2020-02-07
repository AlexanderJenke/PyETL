# Decubitus risk prediction

## model raining 
Frist step:
```python3
python3 prepare_dataset.py 
```

Second step:
```python3
python3 main.py 
```

Third step:
```python3
python3 find_th.py 
```

Fourth step:
```python3
python3 create_model_file.py [state_dict] [threshold] [disease_lut] 
```


## prediction pipeline

Simply run:
```python3
python3 main.py [PATH TO MODEL_FILE]
```

To set up a regular execution use:
```python3
python3 cron.py
```