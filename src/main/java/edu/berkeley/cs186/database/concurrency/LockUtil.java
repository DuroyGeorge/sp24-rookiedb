package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;


/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     * <p>
     * `requestType` is guaranteed to be one of: S, X, NL.
     * <p>
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     * lock type can be, and think about how ancestor looks will need to be
     * acquired or changed.
     * <p>
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }
        if (hasXAncestor(lockContext, transaction)) return;
        if (requestType == LockType.X) {
            isGoodForX(parentContext, transaction);
            switch (effectiveLockType) {
                case X:
                    break;
                case IX:
                    lockContext.escalate(transaction);
                    break;
                case IS:
                    lockContext.escalate(transaction);
                    lockContext.promote(transaction, LockType.X);
                    break;
                case S:
                case SIX:
                    lockContext.promote(transaction, LockType.X);
                    break;
                case NL:
                    lockContext.acquire(transaction, LockType.X);
                    break;
            }
        } else if (requestType == LockType.S) {
            switch (effectiveLockType) {
                case S:
                    break;
                case IS:
                    isGoodForS(parentContext, transaction);
                    lockContext.escalate(transaction);
                    break;
                case IX:
                    lockContext.promote(transaction,LockType.SIX);
                    break;
                case NL:
                    isGoodForS(parentContext, transaction);
                    lockContext.acquire(transaction, LockType.S);
                    break;
            }
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    private static boolean hasXAncestor(LockContext parentContext, TransactionContext transaction) {
        if (parentContext == null) return false;
        LockType parentLockType = parentContext.getEffectiveLockType(transaction);
        if (parentLockType == LockType.NL) {
            return hasXAncestor(parentContext.parentContext(), transaction);
        }
        return false;
    }

    private static void isGoodForX(LockContext parentContext, TransactionContext transaction) {
        if (parentContext == null) return;
        LockType parentLockType = parentContext.getEffectiveLockType(transaction);
        switch (parentLockType) {
            case NL:
                isGoodForX(parentContext.parentContext(), transaction);
                parentContext.acquire(transaction, LockType.IX);
                break;
            case IS:
                isGoodForX(parentContext.parentContext(), transaction);
                parentContext.promote(transaction, LockType.IX);
                break;
            case S:
                isGoodForX(parentContext.parentContext(), transaction);
                parentContext.promote(transaction, LockType.SIX);
                break;
        }

    }

    private static void isGoodForS(LockContext parentContext, TransactionContext transaction) {
        if (parentContext == null) return;
        LockType parentLockType = parentContext.getEffectiveLockType(transaction);
        if(parentLockType == LockType.NL) {
            isGoodForS(parentContext.parentContext(), transaction);
            parentContext.acquire(transaction, LockType.IS);
        }
    }
}
