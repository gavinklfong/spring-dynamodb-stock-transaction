package space.gavinklfong.stock.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import space.gavinklfong.stock.dto.Ticket;
import space.gavinklfong.stock.dto.TicketReservation;
import space.gavinklfong.stock.model.StockTransaction;
import space.gavinklfong.stock.service.StockService;

import java.util.List;

@RequiredArgsConstructor
@RestController
public class StockController {

    private final StockService stockService;

    @GetMapping("/accounts/{accountNumber}/stocks/transactions")
    public List<StockTransaction> retrieveStockTransactions(@PathVariable String accountNumber) {
        return stockService.findTheatreShow(showId)
                .orElseThrow();
    }

    @GetMapping("/shows/{showId}/tickets")
    public List<Ticket> retrieveTickets(@PathVariable String showId) {
        return stockService.findTickets(showId);
    }

    @PostMapping("/shows/{showId}/tickets/{ticketId}/reserve")
    public TicketReservation reserveTicket(@PathVariable String showId, @PathVariable String ticketId) {
        return TicketReservation.builder()
                .reference(stockService.reserveTicket(showId, ticketId))
                .showId(showId)
                .ticketId(ticketId)
                .build();
    }
}
