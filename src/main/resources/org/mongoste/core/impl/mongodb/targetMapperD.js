function () {
    var k = {};
    k.idc = this._idc;//EVENT_CLIENT_ID
    k.ida = this._ida;//EVENT_ACTION
    k.idk = this._idk;//EVENT_TARGET_TYPE
    k.idt = this._idt;//EVENT_TARGET
    k.own = this.own || [];//EVENT_TARGET_OWNERS
    k.tags = this.tags|| [];//EVENT_TARGET_TAGS
    var metaUnique = "ip";
    for (var d in this.days) {
        var day = this.days[d];        
        for (var h in day.hours) {
            var hour = day.hours[h];            
            k.date = new Date();
            k.date.setUTCFullYear(this.y);
            k.date.setUTCMonth(this.m-1);
            k.date.setUTCDate(d);
            k.date.setUTCHours(0);
            k.date.setUTCMinutes(0);
            k.date.setUTCSeconds(0);
            k.date.setUTCMilliseconds(0);                      
            for (var meta in hour.meta) {
                if(meta == metaUnique) {
                    var muHour = hour.meta[metaUnique];
                    for (var mu in muHour) {
                        emit(k, { count: 0 , unique : 1 });
                    }
                }
            }            
            emit(k, { count : hour.count , unique : 0 });
        }
    }
}