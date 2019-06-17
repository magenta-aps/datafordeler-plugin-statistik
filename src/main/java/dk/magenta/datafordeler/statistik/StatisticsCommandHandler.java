package dk.magenta.datafordeler.statistik;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dk.magenta.datafordeler.core.PluginManager;
import dk.magenta.datafordeler.core.command.Command;
import dk.magenta.datafordeler.core.command.CommandData;
import dk.magenta.datafordeler.core.command.CommandHandler;
import dk.magenta.datafordeler.core.command.Worker;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.core.exception.DataStreamException;
import dk.magenta.datafordeler.core.exception.InvalidClientInputException;
import dk.magenta.datafordeler.statistik.services.*;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;

/**
 * A CommandHandler for executing pulls. The command interface delegates to this
 * when it receives a "pull" command
 */
@Component
public class StatisticsCommandHandler extends CommandHandler {

    @Value("${dafo.statistics.enabled:false}")
    private boolean enabled;

    @Autowired
    private BirthDataService birthDataService;
    @Autowired
    private DeathDataService deathDataService;
    @Autowired
    private CivilStatusDataService civilStatusDataService;
    @Autowired
    private MovementDataService movementDataService;
    @Autowired
    private StatusDataService statusDataService;

    private static Logger log = LogManager.getLogger(StatisticsCommandHandler.class.getCanonicalName());

    public static class StatisticsCommandData extends CommandData {

        public StatisticsCommandData() {
        }

        @JsonProperty(required = true)
        public String type;


        private ObjectNode data = new ObjectMapper().createObjectNode();

        @JsonAnySetter
        public void setData(String key, JsonNode value) {
            this.data.set(key, value);
        }

        public ObjectNode getData() {
            return this.data;
        }

        @Override
        public boolean containsAll(Map<String, Object> data) {
            if (data != null) {
                for (String key : data.keySet()) {
                    if (key.equals("type") && this.type != null && this.type.equals(data.get("type"))) {
                        // Ok for now
                    } else {
                        // This must not happen. It means there is an important difference between the incoming map and this object
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public Map<String, Object> contents() {
            return Collections.singletonMap("type", this.type);
        }
    }

    public class StatisticsWorker extends Worker {

        private StatisticsService.ServiceName serviceName;

        private Filter filter;

        public StatisticsWorker(StatisticsService.ServiceName serviceName, Filter filter) {
            this.serviceName = serviceName;
            this.filter = filter;
        }

        @Override
        public void run() {

            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
            String formatDateTime = now.format(formatter);

            if (StatisticsService.PATH_FILE != null) {
                try {
                    File file = new File(StatisticsService.PATH_FILE, serviceName.name().toLowerCase() + "_" + formatDateTime.toString() + ".csv");
                    file.createNewFile();
                    FileOutputStream outputStream = new FileOutputStream(file);
                    String outputDescription = "Written to file " + file.getCanonicalPath();
                    switch (this.serviceName) {
                        case BIRTH:
                            StatisticsCommandHandler.this.birthDataService.run(filter, outputStream);
                            break;
                        case DEATH:
                            StatisticsCommandHandler.this.deathDataService.run(filter, outputStream);
                            break;
                        case CIVILSTATUS:
                            StatisticsCommandHandler.this.civilStatusDataService.run(filter, outputStream);
                            break;
                        case MOVEMENT:
                            StatisticsCommandHandler.this.movementDataService.run(filter, outputStream);
                            break;
                        case STATUS:
                            StatisticsCommandHandler.this.statusDataService.run(filter, outputStream);
                            break;
                        case ROAD:
                            StatisticsCommandHandler.this.statusDataService.run(filter, outputStream);
                            break;
                        case LOCALITY:
                            StatisticsCommandHandler.this.statusDataService.run(filter, outputStream);
                            break;
                    }
                    log.info(outputDescription);
                } catch (IOException e) {
                    log.error("Statistics generation errored", e);
                    this.onError(e);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Autowired
    private PluginManager pluginManager;

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    protected String getHandledCommand() {
        return "statistics";
    }

    public boolean accept(Command command) {
        return this.enabled;
    }

    @Override
    public Worker doHandleCommand(Command command) throws DataFordelerException {
        if (this.accept(command)) {
            this.getLog().info("Handling command '" + command.getCommandName() + "'");

            StatisticsCommandData commandData = this.getCommandData(command.getCommandBody());

            StatisticsWorker worker = new StatisticsWorker(
                    StatisticsService.ServiceName.valueOf(commandData.type.toUpperCase()),
                    new Filter(commandData.getData())
            );

            worker.setUncaughtExceptionHandler((th, ex) -> StatisticsCommandHandler.this.getLog().error("Statistics generation failed", ex));
            return worker;
        }
        return null;
    }

    public StatisticsCommandData getCommandData(String commandBody)
            throws DataStreamException, InvalidClientInputException {
        try {
            StatisticsCommandData commandData = this.objectMapper.readValue(commandBody, StatisticsCommandData.class);
            this.getLog().info("Command data parsed");
            return commandData;
        } catch (IOException e) {
            InvalidClientInputException ex = new InvalidClientInputException("Unable to parse command data '" + commandBody + "'");
            this.getLog().error(ex);
            throw ex;
        }
    }

    public ObjectNode getCommandStatus(Command command) {
        return objectMapper.valueToTree(command);
    }
}
