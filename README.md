# reactor-relational-operators
Different relational join operators implementations.
Based on usage you can choose between ```Inner Loop Join```, ```Sorted Merge Join``` and ```Hash Match Join```
Currently pure Jackson data binding JsonNode streams supported.

- ```Inner Loop Join``` 
    - restricted by memory for inner right data
    - no size constaint on left data set
- ```Sorted Merge Join```
    - requires sorted left and right data set
    - no size constraints on left and right data set
- ```Hash Match Join```
    - restricted by memory for hashed left data
    - no size constraint for right data set

