function () {
    var k = {};
    k.idc = this._idc;//EVENT_CLIENT_ID
    k.ida = this._ida;//EVENT_ACTION
    k.idk = this._idk;//EVENT_TARGET_TYPE
    k.idt = this._idt;//EVENT_TARGET
    var metaUnique = "ip";
    var d;
    for (d in this.days) {
        var day = this.days[d];
        var h;
        for (h in day.hours) {
            var hour = day.hours[h];            
            k.date = new Date();
            k.date.setUTCFullYear(this.y);
            k.date.setUTCMonth(this.m-1);
            k.date.setUTCDate(d);
            k.date.setUTCHours(h);
            k.date.setUTCMinutes(0);
            k.date.setUTCSeconds(0);
            k.date.setUTCMilliseconds(0);
            var meta;
            for (meta in hour.meta) {
                if(meta == metaUnique) {
                    var muHour = hour.meta[metaUnique];
                    var mu;
                    for (mu in muHour) {
                        emit(k, { count: 0 , unique : 1 });
                    }
                }
            }            
            emit(k, { count : hour.count , unique : 0 });
        }
    }
}