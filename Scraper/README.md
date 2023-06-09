#### Unfortunately this program only works on Windows environments due to the differences between UNIX and WIN path definitions.
 - The program can be converted to be used in UNIX environments simply by changing all "\\" path definitions to "/" in the code - which there aren't many of.

### Installation & Usage
Install requirements

```bash
pip install -r requirements.txt
```

Start the program
```bash
python main.py
```


### Todo
Since SQL insert module was not yet available, it needs to be added if data is to be uploaded to a DB.

In file components/filehandler.py, scraped data is to be inserted on row 11 - this is also reminded when the program runs.
