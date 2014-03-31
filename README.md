regnum-address
==============
In this project, we try to find company's website address.

There is information(name, address, register number etc.) abaout companies which is registered UK in http://download.companieshouse.gov.uk/cy_output.html. We use this data to get company's register number and name as a input, because register number is unique for companys, so we use register number to find compnay's website address. 

Our second input is common crawl data which is in text format. We try to find register numbers in common crawl data and if URL of website include company's name, it means we found website address.

