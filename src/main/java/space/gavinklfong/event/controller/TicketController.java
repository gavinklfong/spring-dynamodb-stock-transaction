package space.gavinklfong.event.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import space.gavinklfong.event.dto.Show;
import space.gavinklfong.event.dto.Ticket;

import java.util.List;
import java.util.Optional;

@RestController
public class TicketController {

    @GetMapping("/shows/{showId}")
    public Show retrieveShows(@PathVariable String showId) {
        return Show.builder().build();
    }

    @GetMapping("/shows/{showId}/tickets")
    public List<Ticket> retrieveTickets(@PathVariable String showId, @RequestParam(defaultValue = "ALL") String status) {
        return List.of(Ticket.builder().build());
    }

    @PostMapping("/shows/{showId}/tickets/{ticketId}/reserve")
    public ResponseEntity<Void> reserveTicket(@PathVariable String showId, @PathVariable String ticketId) {
        return ResponseEntity.ok().build();
    }

    @PostMapping("/shows/{showId}/tickets/{ticketId}/release")
    public ResponseEntity<Void> releaseTicket(@PathVariable String showId, @PathVariable String ticketId) {
        return ResponseEntity.ok().build();
    }
}
