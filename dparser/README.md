# Dandelion Composition Language

Compositions in Dandelion descirbe how data is passed between functions.

## Sets and Items
To describe input and output of functions we use sets and items.
The dataflow between functions is described in terms of data sets, which are unordered collections of items.
Each set has a name and can contain an arbitrary number of items.
Each item also has a name in addition to a integer key.
Keys are used for grouping items within set to express parallelism in dataprocessing.

## Function declarations
All function declarations need to come before the composition that uses them.
A function declaration starts with the `function` keyword followed by the name of the function.
After that comes a comma separated list of input set names in parenthesis, a `=>` and a comma separated list of output set names in parenthesis. 
For example the system function for HTTP requests can be declared as:
```
function HTTP(Request) => (Response);
```

The names of the sets in the function declaration do not need to match the names that are used when the function is registered.
For passing sets into the function, they are enumerated, meaning the first set given in the declaration will be set 0 in the function and will be visible to the function at runtime with the name of the set with index 0 when registering the sets.
This is on purpose, to allow compositions to use more descriptive names, even when functions need specific set names internally for techincal reasons.

## Composition declarations
A composition has the same signature as a function, to allow other compositions to use them as if they were functions.
The declaration differs in, that compositions use the `composition` keyword instead of `function` and they then have a body surrounded by `{<body>}`
The body contains a list of function applications.
A function application starts with the name of a function declared earlier followed by a comma separated list of input set assignments surrounded by `()`,
followed by a `=>`,
a comma separated list of output set assignments surrounded by `()`,
terminated by `;`.
Example:
```
<function name>
    (<input set name> = <parallelization keyword> <composition set name>, ...)
        =>
    (<composition set name> = <output set name>, ...);
```

The input and output set names are the set names given in the function declrataion.
Composition sets can be defined either in the composition signature as composition input or output sets, or when a function application produces an output which is assigned to a not yet defined composition set name.
A composition set name can only be assigned to as output by a single function, but can be used as input by an arbitrary number of functions. 
Functions can be applied multiple times in the same function.
The parallelization keywords are further described [here](###paralelization-keywords) together with the `by` clause that can be added at the end of a applicaition. 

An example of a composition, using the HTTP function and the following:
- classification, tagging each picture with a key, outputting the pictures with the assiziated keys as well as a list of HTTP GET request to fetch existing archives containing pictures with the same keys.
- compression, reads the HTTP response, if there is an archive for the key of the pictures to compress, adds all pictures into that archive otherwise compresses them into a new one. Then creates an HTTP PUT request to upload the update/new archive to storage
```
function classify (pics) => (tagged_pics, archives);
function compress (pics, archives) => (put_requests);
function HTTP(requests) => (responses);

compositon image_process (images) => (status) {
    classify (pics = all images) => (
        classified = tagged_pics,
        fetch_requests = archives
    );
    
    HTTP (requests = each fetch_requests) =>
        (fetched_archives = responses);
    
    compress (
        pics = keyed classified,
        archives = keyed fetched_archives
    ) => (
        store_requests = put_requests    
    ) by classified left fetched_archives;
    
    HTTP (requests = each store_requests) =>
        (status = responses);
}
```

### Parallelization keywords

There are 3 keywords to express how sets should be parallelized over.
- `all`, a function should get all items in this set.
- `each`, a function should get a single item from this set, i.e. run a separate function for each item in the set.
- `keyed`, a function should get all items from the set with the same key, i.e. run a separate function for each distinc key in the set.

If a function has multiple input sets, by default the cross product of functions is spawned.
For any combination of `all` with another keyword, this will spawn as many parallel functions as the other keyword specifies.

For combinatons with `each`, this spawns a function for each function the other keyword asks for.
For another `each` this spawns a function for each possible pairing of items, for another `keyed` it spawns a function for each combination of item and key group.

When two `keyed` are combined, the default is to spawn a function for each combination of keys from the two sets.
This can be further refined by adding a `by` clause at the end of the function application to filter.
The `by` is followed by a set name, a join type and another set name. When there are more than two sets, another join type and another set name can be added, where the sets are joined from left to right.
The join types are:

- `inner`, spawn a function only for keys present in both sets.
- `left`, spawn a function for each key in the left set, adding items from the set on the right if they have the same key.
- `right`, spawn a function for each key in the right set, adding items form the left set if they have the same key.
- `outer`, spawn a function for each key in either set, a function gets all items with the same key from any of the two sets.
- `cross`, spawn a function for each cobination of one key from left set with one key from right set.

Where `cross` describes the default behaviour.
