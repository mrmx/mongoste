function (key,vals) {
    //print("Target reducing key:"+tojson(key));
    var total = 0;
    var unique = 0;
    var val;
    for(var i in vals) {
        val = vals[i];
        total += val.count;
        unique += val.unique;
    }
    return {count : total, unique : unique};
}