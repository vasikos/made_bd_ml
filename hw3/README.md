## How to run

First command line argument is the name of file with data. First column is recognized as target variable and others as features.
Second argument is optional: if it equals to "add_const" - a constant column would be added to features to account for an intercept

test_file.csv consists of 6 columns, first is a noise plus weighted sum of the next 5 with weights equal to [1, 2, 3, 4, 5]. Features are randomly generated.
