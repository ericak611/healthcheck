import React, { useEffect, useState } from 'react';
import '../App.css';

export default function EndpointAudit(props) {
    const [isLoaded, setIsLoaded] = useState(false);
    const [log, setLog] = useState(null);
    const [error, setError] = useState(null);
    const [index, setIndex] = useState(null);

    useEffect(() => {
        const rand_val = Math.floor(Math.random() * 100); // Get a random event from the event store
        setIndex(rand_val);

        const getAudit = () => {
            fetch(`http://acit3855-system.eastus.cloudapp.azure.com:8110/${props.endpoint}?index=${rand_val}`)
                .then((res) => res.json())
                .then(
                    (result) => {
                        console.log("Received Audit Results for " + props.endpoint);
                        setLog(result);
                        setIsLoaded(true);
                    },
                    (error) => {
                        setError(error);
                        setIsLoaded(true);
                    }
                );
        };

        const interval = setInterval(() => getAudit(), 4000); // Update every 4 seconds
        return () => clearInterval(interval);
    }, [props.endpoint]);

    if (error) {
        return <div className={"error"}>Error found when fetching from API</div>;
    } else if (!isLoaded) {
        return <div>Loading...</div>;
    } else {
        return (
            <div>
                <h3>{props.endpoint}-{index}</h3>
                {JSON.stringify(log)}
            </div>
        );
    }
}

