// 浏览器用户数据收集sdk
(function(){
  // 操纵cookie的对象
  var CookieUtil={
  	// 通过cookie的name获取cookie
  	get: function(name) {
  	  var cookieName=encodeURIComponent(name)+"=",
  	  		startIndex=document.cookie.indexOf(cookieName),
  	  		cookieValue=null;
  	  if(startIndex>-1){
  	  	var endIndex=document.cookie.indexOf(";", startIndex);
  	  	if(endIndex==-1){
  	  		endIndex=document.cookie.length;
  	  	}
  	  	cookieValue=decodeURIComponent(document.cookie.substring(startIndex+cookieName.length,endIndex));
  	  }
  	  return cookieValue;
  	},
  	// 设置cookie信息
  	set:function(name,value,expires,path,domain,secure){
  	  var cookieValue=encodeURIComponent(name)+"="+encodeURIComponent(value);
  	  
  	  if(expires){
  	  	var expiresTime=new Date();
  	  	expiresTime.setTime(expires);
  	  	cookieValue+=";expires="+expiresTime.toGMTString();
  	  }
  	  
  	  if(path){
  	  	cookieValue+=";path="+path;
  	  }
  	  
  	  if(domain){
  	  	cookieValue+=";domain="+domain;
  	  }
  	  
  	  if(secure){
  	  	cookieValue+=";secure="+secure;
  	  }
  	  document.cookie=cookieValue;
  	},
  	setExt:function(name,value,expire,path){
  	  this.set(name,value,new Date().getTime()+315360000000,"/");
  	}
  };
	// 收集数据的对象
	var tracker={
		// 基本属性
		clientConfig: {
			accessUrl: "http://big-data01.hut.edu/logo.png",
			sessionTimeOut: 360, // 6mins
			maxWaitTime: 3600, // 1h
			version: "0.0.1"
		},
		// 公共属性
		commons: {
			eventName: "en",
			version: "ver",
			platform: "pl",
			sdk: "sdk",
			uuid: "u_ud",
			memberId: "u_mid",
			sessionId: "u_sd",
			clientTime: "c_time",
			resolution: "b_rst",
			userAgent: "b_iev",
			currentUrl: "p_url",
			referrerUrl: "p_ref",
			title: "tt",
			language: "l",
			orderId: "oid",
			orderName: "on",
			currencyAmount: "cua",
			currencyType: "cut",
			payType: "pt",
			category: "ca",
			action: "ac",
			kv: "kv_",
			duration: "du"
		},
		
		keys: {
			pageView: "e_pv",
			chargeRequestEvent: "e_crt",
			launch: "e_l",
			eventDurationEvent: "e_e",
			sid: "analysis_sid",
			uuid: "analysis_uuid",
			mid: "analysis_mid",
			preVisitTime: "previsit"
		},
		
		/*
		 * 会话idget/set
		 */
		getSid: function() {
			return CookieUtil.get(this.keys.sid);
		},
		
		setSid: function(sid) {
			if(sid){
				CookieUtil.setExt(this.keys.sid, sid);
			}
		},
		/*
		 * uuid get/set
		 */
		getUuid: function(){
			return CookieUtil.get(this.keys.uuid);
		},
		
		setUuid: function(uuid){
			if(uuid){
				CookieUtil.setExt(this.keys.uuid, uuid);
			}
		},
		
		// 会员id get/set
		getMemberId: function(){
			return CookieUtil.get(this.keys.mid);
		},
		
		setMemberId: function(mid){
			if(mid){
				CookieUtil.setExt(this.keys.mid, mid);
			}
		},
		/*
		 * js加载时就开始调用该方法，如果是新用户就创建一个session，
		 * 否则先判断session是否过期，过期就创建一个新的session.未
		 * 过期就更新session访问时间。最后，在所有方法执行成功后，触
		 * 发pv事件
		 */
		startSession: function(){
			// 如果为true，说明该用户不是第一次访问
			if (this.getSid()) {
				// 那么判断session是否过期
				if (this.isSessionTimeout()) {
					// true表示session过期，所以创建一个新的session
					this.createSession();
				} else {
					// flase表示session未过期，更新session访问时间
					this.updatePreVisitTime(new Date().getTime());
				}
			} else {
				console.log("before call pv...");
				// 表示用户是第一次访问，应该创建一个session
				this.createSession();
			}
			console.log("call pv...");
			// 用户进入页面就会触发pv事件
			this.onPageView();
		},
		/*
		 * 用户第一次访问网页将会触发onLaunch事件
		 */
		onLaunch: function(){
			var launch = {};
			launch[this.commons.eventName] = this.keys.launch;
			this.setCommonColumns(launch);
			this.sendDataToServer(this.parseParam(launch));
		},
		/*
		 * 用户访问一次网页就会触发一次pv事件
		 */
		onPageView:function(){
			if(this.preCallApi()){
				var time=new Date().getTime();
				var pageViewEvent={};
				console.log("set eventName...");
				pageViewEvent[this.commons.eventName]=this.keys.pageView;
				pageViewEvent[this.commons.currentUrl]=window.location.href; // 当前页面
				pageViewEvent[this.commons.referrerUrl]=document.referrer; // 上一个页面
				pageViewEvent[this.commons.title]=document.title; // 标题
				console.log(pageViewEvent);
				this.setCommonColumns(pageViewEvent); // 设置公共属性
				this.sendDataToServer(this.parseParam(pageViewEvent)); //发送编码后的数据
				this.updatePreVisitTime(time);
			}
		},
		/*
		 * 用户下订单触发的事件
		 */
		onChargeRequest:function(orderId, orderName, currencyAmount, currencyType, payType){
			if (this.preCallApi()) {
				if (!orderId || !currencyType || !payType) {
					this.log("订单id、货币类型、支付方式不能为空...");
					return ;
				}
				// 金额必须是数字
				if (typeof(currencyAmount) == "number") {
					var time = new Date().getTime();
					var chargeRequestEvent = {};
					chargeRequestEvent[this.commons.eventName] = this.keys.chargeRequestEvent;
					chargeRequestEvent[this.commons.orderId] = orderId;
					chargeRequestEvent[this.commons.orderName] = orderName;
					chargeRequestEvent[this.commons.currencyAmount] = currencyAmount;
					chargeRequestEvent[this.commons.currencyType] = currencyType;
					chargeRequestEvent[this.commons.payType] = payType;
					this.setCommonColumns(chargeRequestEvent);
					this.sendDataToServer(this.parseParam(chargeRequestEvent));
					this.updatePreVisitTime(time);
				} else {
					this.log("订单金额必须是数字...");
					return ;
				}
			}
		},
		/*
		 * 具体业务事件
		 */
		onEventDuration:function(category, action, map, duration){
			if (this.preCallApi()) {
				if (category && action) {
					var time = new Date().getTime();
					var event = {};
					event[this.commons.eventName] = this.keys.eventDurationEvent;
					event[this.commons.category] = category;
					event[this.commons.action] = action;
					if (map) {
						for (var k in map) {
							if (k && map[k]) {
								event[this.commons.kv + k] = map[k]; 
							}
						}
					}
					if (duration) {
						event[this.commons.duration] = duration;
					}
					this.setCommonColumns(event);
					this.sendDataToServer(this.parseParam(event));
					this.updatePreVisitTime(time);
				} else {
					this.log("事件类型和操作不能为空...");
					return ;
				}
			}
		},
		/*
		 * 设置公共信息
		 */
		setCommonColumns: function(data){
			data[this.commons.version]=this.clientConfig.version;
			data[this.commons.platform]="website";
			data[this.commons.sdk]="js";
			data[this.commons.uuid]=this.getUuid(); //设置用户id
			data[this.commons.memberId]=this.getMemberId(); // 设置会员id
			data[this.commons.sessionId]=this.getSid(); // 设置会话id
			data[this.commons.clientTime]=new Date().getTime(); // 客户端时间
			data[this.commons.language]=window.navigator.language; // 语言
			data[this.commons.userAgent]=window.navigator.userAgent; // 浏览器类型
			data[this.commons.resolution]=screen.width+"*"+screen.height; // 屏幕分辨率
			console.log(data);
		},
		/*
		 * 判断session是否过期，每次触发事件前，都调用该方法
		 */
		preCallApi: function(){
			if(this.isSessionTimeout()){
				// 如果为true，说明session已过期，需要重新创建session
				this.createSession();
			} else {
				this.updatePreVisitTime(new Date().getTime());
			}
			return true;
		},
		/*
		 * 发送数据
		 */
		sendDataToServer: function(data){
			var imag=new Image(1,1);
			imag.src=this.clientConfig.accessUrl+"?"+data;
		},
		/*
		 * 对数据进行编码
		 */
		parseParam: function(data){
			console.log("enter console...");
			console.log(data);
			var params="";
			for(var e in data){
				if(e && data[e]){
					console.log(e + "=" + data[e]);
					params += encodeURIComponent(e)+"="+encodeURIComponent(data[e])+"&";
				}
			}
			if(params){
				return params.substring(0, params.length - 1);
			} else {
				return params;
			}
		},
		/*
		 * 生成id
		 */
		generateId: function(){
			var chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
			var tmpid = [];
			var random;
			tmpid[8] = tmpid[13] = tmpid[18] = tmpid[23] = '-';
			tmpid[14] = '4';
			for (i = 0; i < 36; i++) {
				if (!tmpid[i]) {
					random = 0 | Math.random() * 16;
					tmpid[i] = chars[(i == 19) ? (random & 0x3) | 0x8 : random];
				}
			}
			return tmpid.join('');
		},
		/*
		 * 更新session最近访问时间
		 */
		updatePreVisitTime: function(time){
			CookieUtil.setExt(this.keys.preVisitTime, time);
		},
		/*
		 * 判断session是否过期
		 */
		isSessionTimeout: function(){
			var time = new Date().getTime();
			var preTime = CookieUtil.get(this.keys.preVisitTime);
			if (preTime) {
				// 判断是否超过6分钟，超过说明session过期，否则session未过期
				return time - preTime > this.clientConfig.sessionTimeOut * 1000;
			}
			return true;
		},
		createSession: function(){
			console.log("createSession...");
			// 获取当前时间
			var time = new Date().getTime();
			// 生成sessionId
			var sid = this.generateId();
			// 设置sessionId
			this.setSid(sid);
			// 设置session过期时间
			this.updatePreVisitTime(time);
			// 判断uuid是否存在，如果不存在，说明用户是第一次访问，需要生成一个uuid
			if (!this.getUuid()) {
				// 生成uuid
				var uuid = this.generateId();
				// 设置uuid
				this.setUuid(uuid);
				// 用户第一次访问网页，触发onLaunch事件
				this.onLaunch();
			}
		},
		/*
		 * 打印日志
		 */
		log: function(msg) {
			console.log(msg);
		}
	};
	
	window.__AE__ = {
		startSession: function() {
			tracker.startSession();
		},
		onPageView: function() {
			tracker.onPageView();
		},
		onChargeRequest: function(orderId, name, currencyAmount, currencyType, paymentType) {
			console.log("enter AE onChargeRequest...");
			tracker.onChargeRequest(orderId, name, currencyAmount, currencyType, paymentType);
		},
		onChargeRefund: function(orderId, name, currencyAmount, currencyType, paymentType) {
			tracker.onChargeRefund(orderId, name, currencyAmount, currencyType, paymentType);
		},
		onEventDuration: function(category, action, map, duration) {
			tracker.onEventDuration(category, action, map, duration);
		},
		setMemberId: function(mid) {
			tracker.setMemberId(mid);
		}
	};
	
	// 自动加载方法
	var autoLoad = function() {
		// 进行参数设置
		var _aelog_ = _aelog_ || window._aelog_ || [];
		var memberId = null;
		for (i=0;i<_aelog_.length;i++) {
			_aelog_[i][0] === "memberId" && (memberId = _aelog_[i][1]);
		}
		// 根据是给定memberid，设置memberid的值
		memberId && __AE__.setMemberId(memberId);
		// 启动session
		__AE__.startSession();
	};

	autoLoad();
  
})();
