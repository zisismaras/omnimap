use quick_js;
use serde::{Serialize, Deserialize};
use std::cell::RefCell;
use anyhow::{Result, Context as ErrorContext, anyhow};

thread_local! {
    static THREAD_CONTEXT: RefCell<Option<Context>> = RefCell::new(None);
}

pub struct ContextBuilder {
    user_code: String
}

impl ContextBuilder {
    ///Creates a new builder that will create contexts preloaded with user_code
    pub fn new(user_code: &str) -> ContextBuilder {
        ContextBuilder {
            user_code: String::from(user_code)
        }
    }

    ///Creates a new js context
    pub fn build(&self) -> Result<Context> {
        let context = quick_js::Context::new().context("Could not create js context")?;
        ContextBuilder::add_runtime_to_context(&context)?;
        context.eval(&self.user_code).context("Could not evaluate js file")?;

        Ok(Context {
            js_context: context
        })
    }

    ///Creates a new js context for the current thread if it does not already exists or reuses it
    pub fn reuse<F>(&self, callback: F)
        where F: FnOnce(&Context)
    {
        THREAD_CONTEXT.with(|cell| {
            let mut context = cell.borrow_mut();
            if context.is_none() {
                context.replace(self.build().unwrap());
            }
            callback(context.as_ref().unwrap());
        });
    }

    fn add_runtime_to_context(context: &quick_js::Context) -> Result<()> {
        context.eval("
            let emited = [];
            function emit(key, value) {
                if (typeof key !== 'string') {
                    key = JSON.stringify(key);
                }
                if (typeof value !== 'string') {
                    value = JSON.stringify(value);
                }
                emited.push({key: key, value: value});
            }
            function mapWrapper(first_line_number, lines) {
                first_line_number = parseInt(first_line_number);
                for (let i = 0; i < lines.length; i++) {
                    map(String(first_line_number), lines[i]);
                    first_line_number += 1;

                }
                const result = JSON.stringify(emited);
                emited = [];
                return result;
            }
            function reduceWrapper(key, arrayAsString, rereduce) {
                const reduced = reduce(key, JSON.parse(arrayAsString), JSON.parse(rereduce));
                if (typeof reduced !== 'string') {
                    return JSON.stringify(reduced);
                } else {
                    return reduced;
                }
            }
        ").context("Could not create js context runtime")?;

        //a sum() helper
        //checks if the passed value is an array or a value list so it can work like sum([1,2,3]) or sum(...[1,2,3]) or sum(1,2,3)
        //doesn't throw any errors, invalid arguments are implicitly returning 0
        context.add_callback("sum", |args: quick_js::Arguments| -> i32 {
            let args = args.into_vec();
            match &args[0] {
                quick_js::JsValue::Array(arr) => {
                    arr
                        .iter()
                        .map(|v| {
                            match v {
                                quick_js::JsValue::String(s) => s.parse::<i32>().unwrap_or_default(),
                                quick_js::JsValue::Int(n) => *n,
                                _ => 0
                            }
                        })
                        .sum()
                },
                _ => {
                    args
                        .iter()
                        .map(|v| {
                            match v {
                                quick_js::JsValue::String(s) => s.parse::<i32>().unwrap_or_default(),
                                quick_js::JsValue::Int(n) => *n,
                                _ => 0
                            }
                        })
                        .sum()
                }
            }
        }).context("Could not create js context runtime")?;
        Ok(())
    }
}

pub struct Context {
    js_context: quick_js::Context
}

///Deserialized json result returned from map functions
#[derive(Serialize, Deserialize, Debug)]
pub struct MapResult {
    pub key: String,
    pub value: String,
}

impl Context {
    ///Checks if the context has map() and reduce() functions defined
    pub fn validate(&self) -> Result<()> {
        let has_map = self.js_context.eval_as::<bool>("
            (function() {
                if (typeof map !== 'function') {
                    return false;
                } else {
                    return true;
                }
            })()
        ").context("Could not validate js context")?;
        if !has_map {
            return Err(anyhow!("No map() function defined in the js file"));
        }
        let has_reduce = self.js_context.eval_as::<bool>("
            (function() {
                if (typeof reduce !== 'function') {
                    return false;
                } else {
                    return true;
                }
            })()
        ").context("Could not validate js context")?;
        if !has_reduce {
            return Err(anyhow!("No reduce() function defined in the js file"));
        }

        Ok(())
    }

    ///Runs the map task for this buffer and return the results
    pub fn run_map(&self, line_number: usize, buf: &str) -> Result<Vec<MapResult>> {
        let lines: Vec<&str> = buf.split("\n").filter(|l| !l.is_empty()).collect();
        let first_line_number = format!("{}", line_number - lines.len() + 1);
        let first_line_number = vec![&first_line_number[..]];
        match self.js_context.call_function(
            "mapWrapper",
            vec![first_line_number, lines]
        ).context("An error was throwed in map()")?.as_str() {
            Some(js_result) => {
                let v: Vec<MapResult> = serde_json::from_str(js_result).with_context(|| format!("Could parse map() result: {}", js_result))?;
                Ok(v)
            },
            None => Ok(vec![])
        }
    }

    ///Runs reduce for key and return the results
    pub fn run_reduce(&self, key: &str, values: &Vec<String>, rereduce: bool) -> Result<String> {
        let js_value = serde_json::to_string(values)?;
        let reduce_result = self.js_context
            .call_function("reduceWrapper", vec![key, &js_value, &rereduce.to_string()])
            .context("An error was throwed in reduce()")?;
        match reduce_result.into_string() {
            Some(result) => Ok(result),
            None => Ok(String::from(""))
        }
    }
}
