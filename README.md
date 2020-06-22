# Omnimap

## About

Omnimap is a [mapreduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) task runner designed to work with small datasets. Small being defined as a dataset that can be stored and processed in its entirety by a single computer. The dataset can range from a few kilobytes to many gigabytes (or even terabytes depending on available disk size).  
Omnimap works by reading data line by line from stdin and writing the results to stdout.  
The map and reduce functions are defined using javascript in a `.js` file.

## Installing

Omnimap is being developed and tested only on `linux-x64` so other platforms are not officially supported.  
There is a prebuilt binary in the [releases](https://github.com/zisismaras/omnimap/releases) page that you can download and start using.

## Example

As a very simple example let's re-implement the `wc` program (word count) using mapreduce.  
We will feed it a text file and get back the total lines, words and characters in the file.  

```js
function map(key, value) {
    emit("lines", 1);
    emit("words", value.split(" ").length);
    emit("characters", value.length);
}

function reduce(key, values, rereduce) {
    return sum(values);
}
```

Save the snippet as `wc.js`.  
Let's run it by feeding an example text file using `cat`:

```bash
cat test.txt | ./omnimap-linux-x64 wc.js
```

The output should be something like this:

```text
characters  134824
lines   240
words   19968
```

In the `map()` function the key is the current line number of the file (which we don't use in this example) and the value is the actual line content.  
We use the builtin `emit()` function 3 times for each line to emit 3 key/value pairs (lines, words, characters).  
In the `reduce()` function we get our key, our values which is an array of numbers in our case and the rereduce flag which we will explain below.  
We then just sum the `values` array using the builtin `sum()` function and return the result.  
The output is written to stdout as `key\tvalue\n` for each key so it can be easily parseable by another program (or omnimap itself!).

## Reduce and rereduce

The `rereduce` parameter is a boolean flag which is a byproduct of how tasks are scheduled under the hood.  
I think the [CouchDB](https://couchdb.apache.org/) explanation of rereduce is very well written so i'll just point you to the [CouchDB docs](https://docs.couchdb.org/en/stable/ddocs/views/intro.html#reduce-rereduce) to see how it works.  
It has some implementation details that are couchdb specific but the general logic applies to omnimap as well.

## Tuning

### TODO

## Building from source

To build from source you will need a copy of Clang and LLVM which can be installed from your package manager
and the [rust](https://www.rust-lang.org/) compiler (version 1.44 and up).  
Clone the repo and run:

```bash
cargo build --release
```

You can then use the executable in `./target/release/`.
