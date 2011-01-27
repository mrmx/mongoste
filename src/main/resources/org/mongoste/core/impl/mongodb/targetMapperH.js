function () {
    metaUnique = "ip";
    var handleCount = function(key,target) {
        for (var meta in target.meta) {
            if(meta == metaUnique) {
                var tUnique = target.meta[metaUnique];
                for (var u in tUnique) {
                    emit(key, {count: 0 , unique : 1});
                }
            }
        }
        emit(key, {count : target.count , unique : 0});
    }
    var k = {};
    k.idc = this._idc;//EVENT_CLIENT_ID
    k.ida = this._ida;//EVENT_ACTION
    k.idk = this._idk;//EVENT_TARGET_TYPE
    k.idt = this._idt;//EVENT_TARGET
    k.own = this.own || [];//EVENT_TARGET_OWNERS
    k.tags = this.tags|| [];//EVENT_TARGET_TAGS
    k.date = new Date();
    k.date.setUTCFullYear(this.y);
    k.date.setUTCMonth(this.m-1);
    k.date.setUTCDate(1);
    k.date.setUTCHours(0,0,0,0);
    if(this.days) {
        for (var d in this.days) {
            var day = this.days[d];
            k.date.setUTCDate(d);
            if(day.hours) {
                for (var h in day.hours) {
                    k.date.setUTCHours(h);
                    handleCount(k,day.hours[h]);
                }
            } else {
                handleCount(k,day);
            }
        }
    } else {
        handleCount(k,this);
    }
}