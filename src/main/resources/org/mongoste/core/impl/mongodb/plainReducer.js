function (key,vals) {
    //print("Reducing key:"+tojson(key));
    var sum = 0;
    for(var i in vals) {
        sum += vals[i].count;
    }
    return {count : sum};
}


