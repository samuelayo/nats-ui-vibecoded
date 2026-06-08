export namespace main {
	
	export class BucketItem {
	    name: string;
	    kind: string;
	    description: string;
	    values: number;
	    bytes: number;
	    history: number;
	    ttl: string;
	    storage: string;
	    replicas: number;
	    compressed: boolean;
	
	    static createFrom(source: any = {}) {
	        return new BucketItem(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.kind = source["kind"];
	        this.description = source["description"];
	        this.values = source["values"];
	        this.bytes = source["bytes"];
	        this.history = source["history"];
	        this.ttl = source["ttl"];
	        this.storage = source["storage"];
	        this.replicas = source["replicas"];
	        this.compressed = source["compressed"];
	    }
	}
	export class ConnectRequest {
	    url: string;
	    username: string;
	    password: string;
	    token: string;
	    credsPath: string;
	
	    static createFrom(source: any = {}) {
	        return new ConnectRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.url = source["url"];
	        this.username = source["username"];
	        this.password = source["password"];
	        this.token = source["token"];
	        this.credsPath = source["credsPath"];
	    }
	}
	export class ConnectionProfile {
	    id: number;
	    name: string;
	    url: string;
	    username: string;
	    password?: string;
	    token?: string;
	    credsPath: string;
	
	    static createFrom(source: any = {}) {
	        return new ConnectionProfile(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.name = source["name"];
	        this.url = source["url"];
	        this.username = source["username"];
	        this.password = source["password"];
	        this.token = source["token"];
	        this.credsPath = source["credsPath"];
	    }
	}
	export class ConsumerItem {
	    streamName: string;
	    name: string;
	    durable: string;
	    filterSubject: string;
	    deliverSubject: string;
	    deliverGroup: string;
	    ackPolicy: string;
	    maxAckPending: number;
	    numPending: number;
	    numAckPending: number;
	    numRedelivered: number;
	    ackFloorStream: number;
	    ackFloorConsumer: number;
	    ackFloorLast?: string;
	    deliveredStream: number;
	    deliveredConsumer: number;
	    deliveredLast?: string;
	    pushBound: boolean;
	
	    static createFrom(source: any = {}) {
	        return new ConsumerItem(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.streamName = source["streamName"];
	        this.name = source["name"];
	        this.durable = source["durable"];
	        this.filterSubject = source["filterSubject"];
	        this.deliverSubject = source["deliverSubject"];
	        this.deliverGroup = source["deliverGroup"];
	        this.ackPolicy = source["ackPolicy"];
	        this.maxAckPending = source["maxAckPending"];
	        this.numPending = source["numPending"];
	        this.numAckPending = source["numAckPending"];
	        this.numRedelivered = source["numRedelivered"];
	        this.ackFloorStream = source["ackFloorStream"];
	        this.ackFloorConsumer = source["ackFloorConsumer"];
	        this.ackFloorLast = source["ackFloorLast"];
	        this.deliveredStream = source["deliveredStream"];
	        this.deliveredConsumer = source["deliveredConsumer"];
	        this.deliveredLast = source["deliveredLast"];
	        this.pushBound = source["pushBound"];
	    }
	}
	export class MessageFilters {
	    subjectContains: string;
	    payloadContains: string;
	    headerKey: string;
	    headerValue: string;
	    limit: number;
	    maxProbes: number;
	    direction: string;
	    startSeq: number;
	
	    static createFrom(source: any = {}) {
	        return new MessageFilters(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.subjectContains = source["subjectContains"];
	        this.payloadContains = source["payloadContains"];
	        this.headerKey = source["headerKey"];
	        this.headerValue = source["headerValue"];
	        this.limit = source["limit"];
	        this.maxProbes = source["maxProbes"];
	        this.direction = source["direction"];
	        this.startSeq = source["startSeq"];
	    }
	}
	export class MessageInfo {
	    sequence: number;
	    subject: string;
	    time: string;
	    data: string;
	    headers: Record<string, Array<string>>;
	    size: number;
	    scheduledAt: string;
	    shard: string;
	    queue: string;
	    job: string;
	    jobId: string;
	
	    static createFrom(source: any = {}) {
	        return new MessageInfo(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.sequence = source["sequence"];
	        this.subject = source["subject"];
	        this.time = source["time"];
	        this.data = source["data"];
	        this.headers = source["headers"];
	        this.size = source["size"];
	        this.scheduledAt = source["scheduledAt"];
	        this.shard = source["shard"];
	        this.queue = source["queue"];
	        this.job = source["job"];
	        this.jobId = source["jobId"];
	    }
	}
	export class ProfileView {
	    id: number;
	    name: string;
	    url: string;
	    username: string;
	    credsPath: string;
	    hasPassword: boolean;
	    hasToken: boolean;
	
	    static createFrom(source: any = {}) {
	        return new ProfileView(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.name = source["name"];
	        this.url = source["url"];
	        this.username = source["username"];
	        this.credsPath = source["credsPath"];
	        this.hasPassword = source["hasPassword"];
	        this.hasToken = source["hasToken"];
	    }
	}
	export class ServerInfoData {
	    name: string;
	    serverId: string;
	    version: string;
	    cluster: string;
	    url: string;
	    address: string;
	    clientId: number;
	    maxPayload: number;
	    authRequired: boolean;
	    jetStreamEnabled: boolean;
	    memory: number;
	    storage: number;
	    streams: number;
	    consumers: number;
	    apiRequests: number;
	    apiErrors: number;
	    inMsgs: number;
	    outMsgs: number;
	    inBytes: number;
	    outBytes: number;
	    reconnects: number;
	
	    static createFrom(source: any = {}) {
	        return new ServerInfoData(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.serverId = source["serverId"];
	        this.version = source["version"];
	        this.cluster = source["cluster"];
	        this.url = source["url"];
	        this.address = source["address"];
	        this.clientId = source["clientId"];
	        this.maxPayload = source["maxPayload"];
	        this.authRequired = source["authRequired"];
	        this.jetStreamEnabled = source["jetStreamEnabled"];
	        this.memory = source["memory"];
	        this.storage = source["storage"];
	        this.streams = source["streams"];
	        this.consumers = source["consumers"];
	        this.apiRequests = source["apiRequests"];
	        this.apiErrors = source["apiErrors"];
	        this.inMsgs = source["inMsgs"];
	        this.outMsgs = source["outMsgs"];
	        this.inBytes = source["inBytes"];
	        this.outBytes = source["outBytes"];
	        this.reconnects = source["reconnects"];
	    }
	}
	export class StreamDetailData {
	    configJSON: string;
	    stateJSON: string;
	    warnings: string[];
	    consumers: ConsumerItem[];
	
	    static createFrom(source: any = {}) {
	        return new StreamDetailData(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.configJSON = source["configJSON"];
	        this.stateJSON = source["stateJSON"];
	        this.warnings = source["warnings"];
	        this.consumers = this.convertValues(source["consumers"], ConsumerItem);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class StreamItem {
	    name: string;
	    subjects: string[];
	    retention: string;
	    storage: string;
	    messages: number;
	    bytes: number;
	    firstSeq: number;
	    lastSeq: number;
	    deleted: number;
	    numSubjects: number;
	    consumers: number;
	    allowDirect: boolean;
	    allowRollup: boolean;
	    allowMsgSched: boolean;
	    allowAtomic: boolean;
	    maxMsgsPerSub: number;
	
	    static createFrom(source: any = {}) {
	        return new StreamItem(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.subjects = source["subjects"];
	        this.retention = source["retention"];
	        this.storage = source["storage"];
	        this.messages = source["messages"];
	        this.bytes = source["bytes"];
	        this.firstSeq = source["firstSeq"];
	        this.lastSeq = source["lastSeq"];
	        this.deleted = source["deleted"];
	        this.numSubjects = source["numSubjects"];
	        this.consumers = source["consumers"];
	        this.allowDirect = source["allowDirect"];
	        this.allowRollup = source["allowRollup"];
	        this.allowMsgSched = source["allowMsgSched"];
	        this.allowAtomic = source["allowAtomic"];
	        this.maxMsgsPerSub = source["maxMsgsPerSub"];
	    }
	}

}

