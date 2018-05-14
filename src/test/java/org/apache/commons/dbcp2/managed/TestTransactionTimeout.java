package org.apache.commons.dbcp2.managed;

import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import org.junit.Assert;
import org.junit.Test;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:zfeng@redhat.com>Zheng Feng</a>
 */
public class TestTransactionTimeout {
    @Test
    public void test(){
        TransactionManager tm = new TransactionManagerImple();
        int i = 0;
        final AtomicInteger x = new AtomicInteger(0);
        final AtomicInteger y = new AtomicInteger(0);

        while(i ++ < 5) {
            try {
                tm.setTransactionTimeout(1);
                tm.begin();
                Transaction tx = tm.getTransaction();
                while (tx != null && tx.getStatus() == Status.STATUS_ACTIVE) {
                    x.getAndIncrement();
                    tx.registerSynchronization(new Synchronization() {
                        @Override
                        public void beforeCompletion() {

                        }

                        @Override
                        public void afterCompletion(int i) {
                            y.getAndIncrement();
                        }
                    });
                }
                tm.commit();
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        Assert.assertEquals(x.get(), y.get());
    }
}
