package cn.sunline.edsp.component.rpc.client.defaultClient;

import cn.sunline.edsp.component.base.utils.LogUtils;
import cn.sunline.edsp.component.rpc.common.RPCParamType;
import cn.sunline.edsp.component.rpc.common.RPCRequest;
import cn.sunline.edsp.component.rpc.common.RPCResponse;
import cn.sunline.edsp.component.rpc.common.exception.RPCTimeoutException;
import cn.sunline.edsp.component.rpc.common.exception.RpcException;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.*;

/**
 * RPC Future <br/>
 *
 * @version 1.0
 */
public class RPCFuture {
	
	private static final Logger sysLog = LogUtils.getSysLogger();
	
	private static final Map<Long, RPCFuture> futures = new ConcurrentHashMap<Long,RPCFuture>();
	
	private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
	
	static {
		executor.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				long time = 0L;
				for (RPCFuture future : futures.values()) {
					if (future.start != Long.MIN_VALUE) {
						time = System.currentTimeMillis() - future.start;
						if (null != future && future.timeout < time) {
							RPCResponse response = new RPCResponse(future.request.getId());
							response.setData(new RPCTimeoutException()); // body
							RPCFuture.received(response);
						}
					}
				}
			}
		}, 30, 30, TimeUnit.MILLISECONDS);
	}
	
	private RPCRequest request;
	private RPCResponse response;
	
	private CountDownLatch latch;
	private long start;
	private long end;
	private long timeout;
	
	public RPCFuture(RPCRequest request, long timeout) {
		this.request = request;
		this.start = Long.MIN_VALUE;
		this.timeout = timeout;
		
		latch = new CountDownLatch(1);
		futures.put(this.request.getId(), this);
	}
	
	public static void start(RPCRequest request) {
		RPCFuture future = futures.get(request.getId());
		future.start = System.currentTimeMillis();
	}
	
	public static void received(RPCResponse response) {
		RPCFuture future = futures.remove(response.getRequestId());
		if (null != future) {
			future.response = response;
			future.latch.countDown();
			future.end = System.currentTimeMillis();
		}
	}

	public RPCResponse get() throws RPCTimeoutException {
		try {
			if (null == response) { // 同步等待
				latch.await();
			}
			
			if (response.getData() instanceof RPCTimeoutException) {
				sysLog.error("rpc service(" + 
						request.getHeader(RPCParamType.service_id.getName()) + ":" +
						request.getHeader(RPCParamType.service_group.getName()) + ":" +
						request.getHeader(RPCParamType.service_version.getName()) + 
						") request timeout(" + 
						this.end + "-" + this.start + "=" + (this.end - this.start) +
						")");
				throw (RPCTimeoutException) response.getData();
			}
			
			return response;
		} catch (InterruptedException e) {
			throw new RpcException(e);
		}
	}
}
