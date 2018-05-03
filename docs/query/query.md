A Wisdom application can be created either using Java API or Wisdom Query Language. Wisdom Query Language is a SQL like query inspired by [Siddhi Query](https://wso2.github.io/siddhi/). A Wisdom query should follow this template:

```text
<app annotation>?
( <stream definition> | <variable definition> | ... ) + 
( <query> ) +
;
```

**NOTE:** Since I am busy with my research, I am unable to list all features implemented in Wisdom here. Once you get access to Wisdom, please check the Unit test classes to see the available features and how to use them. You can contact me at any time to clarify your issues.