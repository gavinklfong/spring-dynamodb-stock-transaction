package space.gavinklfong.stock.model;

import lombok.Builder;
import lombok.Value;

import java.util.List;

@Builder
@Value
public class QueryResultPage <R, K> {
    List<R> results;
    K nextQueryKey;
}
