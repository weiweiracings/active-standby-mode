package person.wei;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Hello world!
 *
 */
public class App 
{
    private static boolean isActive = false;

    /**
     * 在本地启动zkServer， 端口2181
     * @param args
     */
    public static void main( String[] args ) throws Exception
    {
        LeaderLatch leaderLatch = new LeaderLatch(getClient(),
                "/demo/master", "client#" + (int) (Math.random() * 100) );
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                isActive = true;
            }

            @Override
            public void notLeader() {
                isActive = false;
            }
        });
        leaderLatch.start();

        while(true){
            if(isActive){
                //主机可以执行主机任务
                System.out.println(leaderLatch.getId() +  ":I am active.");
            }else{
                //备机可以执行备机任务，并且备机知道主机是谁
                String id = "none";
                try{
                    id = leaderLatch.getLeader().getId();
                }catch (Exception e){
                }
                System.out.println(leaderLatch.getId() +  ":I am standby. I know leader:"
                        + id);
            }

            Thread.sleep(100);
        }

    }

    private static CuratorFramework getClient() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(6000)
                .connectionTimeoutMs(3000)
                .namespace("demo")
                .build();
        client.start();
        return client;
    }
}
