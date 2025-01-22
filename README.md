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

If you want to get a feel for the data, you may want to convert the bincode file to a json file via: 
```bash
trade_networks enrichment-to-json --file e15.bincode --item-code 15 
```
which will create a file called 15.json that is human readable.

## Measuring

### Random Disruptions

#### Calculating Averages

To measure random disruptions use:
```bash
trade_networks shock-cloud-all --threads 24 --json config.json -q
```

Adjust the number of threads to whatever many threads make sense for your computer.
The file config.json contains the parameter for the random disruptions:
```json
{
    "enrich_glob": "e*.bincode",
    "network_glob": "*bincode",
    "years": {
        "start": 2018,
        "end": 2022
    },
    "iterations": 1300,
    "top": 3,
    "unstable_country_threshold": 0.7,
    "original_avail_filter": 1e-9,
    "cloud_steps": 100,
    "cloud_m": 8000,
    "seed": 1824793,
    "reducing_factor": 0.1,
    "hist_bins": 100,
    "id_file": "Trade_DetailedTradeMatrix_E_ItemCodes.csv"
}
```

The parameters:
* "enrich_glob" specifies a globbing for the files that contain the Production data
* "network_glob" specifies a globbing for the files that contain the network data
* "id_file" the item codes file you downloaded earlier
* "top" how many top exporters you want to disturb
* "iterations" how many iterations to perform until we consider our simulation to be converged
* "unstable_country_threshold" - theta. I.e., the threshold for counting countries as severely affected
* "original_avail_filter" - we ignore countries that have less than this amount of product available to themselves before the shocks
* "cloud_steps": how many rho do we target? I think it makes sense to set this equal to "hist_bins"
* "cloud_m": How many samples per cloud_step
* "seed": seed for the rng
* "reducing_factor": Did something at some point, but, as far as I can tell, it is only used for naming the output file at this point.
* "hist_bins" Number of bins for the averaging
* "years": specify the years you are interested in

The result will be files like:
_Y2020_Th0.7_R0.1.dat
Which contains all samples, i.e., the first column is 1-rho and the second column is 
the number of severely affected countries for this sample. 
(Clearly all samples with rho=0 or rho=1 have the same number of severely affected countries)

Additionally the result of this will be files like "_Y2019_Th0.7_R0.1.average" - they will appear in folders corresponding to the item code.
The files contain the following columns:
1) interval_left: left border of 1-rho 
2) interval_right: right border of 1-rho -> specifies the rho range we averaged over
3) hits: how many samples we had in the range
4) average The average number of severely affected countries
5) variance: corresponding variance 
6) average_normed_by_max: column 4 divided by the maximum of column 4 
7) average_normed_by_trading_countries: column 4 divided by N (number of countries that trade in the item, fulfill the "original_avail_filter" and are not top exporters)

Now, if you are interested in averaging these over all the items, e.g. for the year 2018, you can call
```bash
trade_networks shock-cloud-calc-averages '*/_Y2018*average' CN_2018_all_item_average.dat --how normal --mode raw -t
```

#### Yearly Differences

Let's say you want to compare the results of two years with one another.
Assuming you just did the average calculation above, you should have a lot of folders named after the item codes and 
in these folders there are the ".average" files. 
To now process all of these at once, you can do the following, where you specify the years you are interested in
```bash
trade_networks shock-cloud-cmp-years '*/*2020*.average' '*/*2021*.average'
```
This gives you (in the respective folders) a file called something like Item51_2020_vs_2021.dat 
that contains the columns:
* total_export_fraction (of the top exporters) (basically 1-rho)
* average_normed_by_max of Y1 minus average_normed_by_max of Y2 (see column 6 of previous section)

If instead you want to compare the country normed data, i.e., use column 7 of previous section instead, use the "-n" option:
```bash
trade_networks shock-cloud-cmp-years '*/*2020*.average' '*/*2021*.average' -n
```
The file (in the respective folders) will be called something like: Country_normed_Item51_2020_vs_2021.dat
it contains the columns:
* total_export_fraction (of the top exporters) (basically 1-rho)
* average_normed_by_max of Y1 minus average_normed_by_max of Y2 (see column 6 of previous section)

Next we can use all these files to calculate the average of the absolute difference between two years. 
For this we call 
```bash
trade_networks shock-cloud-calc-averages '*/C*Item*2020*2021*' CN_abs_average_2020_vs_2021.dat --how abs --mode processed
```
So for each bin we will average the absolute value over all items.
The resulting file is named as chosen, i.e., CN_abs_average_2020_vs_2021.dat
It will contain:
* 1-rho 
* average over all items
* median over all items

#### Calculation of G

Next we want to calculate G for each year and item. 
For a given year Y we want to sort all items according to the G value of (Y,Y+1),
all other years should also be sorted according to year Y, such that we can do 
our scatter plot.

For this use:
```bash
trade_networks sort-comp-multi-years 2018 2022 --how abs-sum-ignore-nan -r -i Trade_DetailedTradeMatrix_E_ItemCodes.csv -c
```
(Results in file CN_From_2018_to_2022_AbsSumIgnoreNan_cmp_val.dat)
in this example Y would be year 2018 and we want to compare all years from 2018 up to 2022.

To calculate G you have to use either --how abs-sum or --abs-sum-ignore-nan.
The latter will cope with NaN values, for instance, if you did a quick test simulation 
and not all bins were hit in one of the previous simulations. 

The "-r" reverses the order, so if you want to list the G values from lowest to highest you have to omit "-r".
The "-i Trade_DetailedTradeMatrix_E_ItemCodes.csv" is optional. It is used to display the human readable name of the item in the final output.

Resulting file contains all items sorted by the G value of 2018 vs 2019. Columns are:
* Item ID
* G 2018 vs 2019
* G 2019 vs 2020
* G 2020 vs 2021
* G 2021 vs 2022
* Human readable item name

The command also creates the files CN_CMP_res_2018_2019_AbsSumIgnoreNan.dat etc.
In there we have a sorted list of G values and the corresponding item codes and names.


### Percentage disruptions

If you want to have proportional disruptions, we have to set the 
parameters for that in a json file, e.g., percentage.json:
```json
{
  "common": {
    "enrich_file": "e15.bincode",
    "network_file": "15.bincode",
    "years": {
      "start": 2000,
      "end": 2022
    },
    "iterations": 10000,
    "item_code": "15",
    "top": 5,
    "unstable_country_threshold": 0.7,
    "original_avail_filter": 0.0
  },
  "extra": {
    "start": 0.0,
    "end": 1.0,
    "amount": 100
  }
}
```

The parameters:
* "enrich_file" the bincode file of the relevant production data
* "network_file" the bincode file of the relevant network data
* "years": Simulate from year start to year end
* iterations: How many iterations for the system to converge
* item code: The item code of the item in question. 
* top: number of top exporters to disturb
* "unstable_country_threshold": theta from the paper
* "original_avail_filter": 0.0 - We exclude countries that, without disruption, have a negative product availability from the severely affected country count
* extra->start: starting value of 1-rho
* extra->end: final value of 1-rho
* amount: Number of samples between start and end

Now the program is called via:
```bash
trade_networks multi-shocks -w percentages -g -q -c -j percentage.json
```
Parameters:
* "-w percentages": Use the percentage disturbance
* "-g" Additionally output group files
* "-q" Non verbose output preferred
* "-c" Automatically compare successive years and output result
* "-j percentage.json": Specify the file that contains our additional parameters

Output of this command:
Files like: _Y2000_Th0.7_percent.dat 
Containing columns:
1) number of disrupting countries (in this case always 5)
2) disruption percentage (1-rho)
3) number of severely affected countries

Files like _Y2000_Th0.7_percent.group

This file contains the ids of the countries that are severely affected in the respective bins of the previous files.
Structure:
§ {Number of disturbed countries; In this example always 5} {1-rho}
id1
id2
…
§{Number of disturbed countries; In this example always 5} {1-rho}
etc.

Files like _Y2000_vs_2001_Th0.7.dat
Containing columns:
1) 1-rho of year 1
2) 1-rho of year 2 (should be, within numerical precision, equal to rho of year 1)
3) Number of countries severely affected in both years
4) number of countries that are severely affected in at least one of the years

The file _Y2000-Y2022_percent.dat
* Column 1: 1-rho
The others are more complex, so bear with me:
* Column X: for the current disruption level, how many countries appear as severely affected in exactly X-1 years?

This file can be used to measure s. For example if we want to generate output with the columns:
1) rho
2) s_4

we can use:
```bash
awk '!/^#/ { sum=0; for(i=5; i<=24; i++) { sum+=$i } print 1-$1, sum }' _Y2000-Y2022_percent.dat
```
Note that awk is a well known program available for basically every Linux distribution
What does this do? We sum the columns 5 until 24 (representing exactly 4 hits through exactly 23 hits) which gives us the number of countries that 
appear in at least 4 of the years in question, i.e., we get s_4


### sequential disruptions


You also need to create a json file for the sequential disruptions.
It is structured exactly the same as the one for the proportional disruptions
with the only difference that now we do not have an "extra".
sequential.json:
```json
{
  "common": {
    "enrich_file": "e15.bincode",
    "network_file": "15.bincode",
    "years": {
      "start": 2000,
      "end": 2022
    },
    "iterations": 100000,
    "item_code": "15",
    "top": 5,
    "unstable_country_threshold": 0.7,
    "original_avail_filter": 0.0
  },
  "extra": null
}
```

The rest is also quite similar:
```bash
trade_networks multi-shocks -w whole-countries -g -q -c -j sequential.json 
```

This creates the files:
_Y2000_vs_Y2001_Th0.7.dat etc
and _Y2000-Y2022_country_count.dat

Which are the same as for the proportional disruptions