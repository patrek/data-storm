package datastorm.eventstore.otientdb;

import org.axonframework.domain.AggregateIdentifier;
import org.axonframework.domain.DomainEvent;

/**
 *
 */
final class SimpleDomainEvent extends DomainEvent {
    private String value;

    SimpleDomainEvent(long sequenceNumber, AggregateIdentifier aggregateIdentifier, String value) {
        super(sequenceNumber, aggregateIdentifier);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SimpleDomainEvent that = (SimpleDomainEvent) o;

        return value.equals(that.value);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
