# Code for: Impact of multiple disruptions on trade networks

## Download Data and change encoding to utf8

This code is used to analyze the data from the FAO.
You can download the data from here: https://www.fao.org/faostat/en/#data/TM and https://www.fao.org/faostat/en/#data/QCL
Make sure to use the bulk download on the right. 

This should give you the files:
Trade_DetailedTradeMatrix_E_AreaCodes.csv
Trade_DetailedTradeMatrix_E_ItemCodes.csv
Trade_DetailedTradeMatrix_E_All_Data_NOFLAG.csv
Production_Crops_Livestock_E_All_Data_NOFLAG.csv

These files are currently not encoded in utf8 but in some other encoding (iso885911 if I recall correctly).
You need to change the encoding into utf8. This can, for example, be done by opening them in vscode, 
clicking on the encoding in the bottom right and selecting "Reopen with Encoding" to first open the file in the 
correct encoding. Then wait a while, the file needs to be processed. Afterwards repeat this, but now click on 
"Save with Encoding" instead and select utf8

## Compiling the program

(Note: You might have to install either of clang or gcc for the below to work)
If you don't have rust (and cargo) installed, install it via: [https://rustup.rs/]
For more detailed instructions see [https://doc.rust-lang.org/stable/book/ch01-01-installation.html]


Then download this repository, for example by clicking on the green "Code" button at the top and 
selecting "Download Zip" (or by cloning the repo via git).

Extract the zip and open a terminal in the "trade_networks" folder
(should be the folder containing the Cargo.toml file).

Then compile the code via:
```bash
cargo b -r
```
The command will automatically download the dependencies of the project (as specified in Cargo.toml)
and compile the program into the executable file "trade_networks" which will appear in the subfolder: target/release/

I recommend adding this file to your PATH to be able to execute the program anywhere you want.

## Parsing Data

### Network Data

The file containing the network data is quite large and you don't want to always read it in for every 
analysis. 
Instead you can read it in once - this creates ".bincode" files for each product, that contain the relevant 
network data.

I recommend reading in all items at once, which can be done with:
```bash
trade_networks parse-all-networks --in-file Path/To/Trade_DetailedTradeMatrix_E_All_Data_NOFLAG.csv -s
```
Note that the "-s" is important. It will skip a few items with mismatched units, that is normal.
This command will create files named {ItemCode}.bincode

Per default this parsing uses the import quantity for the trade data. You can also choose the export quantity (or value)
instead by changing the read-type. 
Run 
```bash
trade_networks parse-all-networks --help
```
to see options.

If you are only interested in a specific product, look up the respective item code in the file 
Trade_DetailedTradeMatrix_E_ItemCodes.csv (item code is the first column) and then use, for example:

```bash
trade_networks parse-networks --in-file Path/To/Trade_DetailedTradeMatrix_E_All_Data_NOFLAG.csv --out 15.bincode --item-code 15
```

Note that reading in the items one by one means that the csv file has to be parsed over and over again.

### Production Data

We also need to parse the extra data we have - we are mainly interested in the production data.
For this use:

```bash
trade_networks parse-all-enrichments -i Path/To/Production_Crops_Livestock_E_All_Data_NOFLAG.csv -o t
```

This parses the data and creates a file called e{ItemCode}.bincode
The option "-o t" specifies that we are only interested in the measurements in tonnes.

You can also parse only individual items by using:

```bash
trade_networks parse-enrichment -e PATH/TO/Production_Crops_Livestock_E_All_Data_NOFLAG.csv --item-code 15 --out e15.bincode
```
instead. 

