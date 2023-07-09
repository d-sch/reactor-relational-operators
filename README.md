# reactor-relational-operators
Different relational join operators implementations.
Based on usage you can choose between ```Inner Loop Join```, ```Sorted Merge Join``` and ```Hash Match Join```

```Inner Loop Join``` restricted by memory for inner right data. no size constaint on left data set
```Sorted Merge Join``` required left and right data set to be sorted, restricted by memory for in-memory sorted lists.
```Hash Match Join``` restricted by memory for hashed left data, no size constraint for right data set

